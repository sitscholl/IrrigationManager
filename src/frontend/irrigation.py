# irrigation.py
from nicegui import ui
from .header import add_header
from .deps import get_db
from .table_editor import TableEditor

@ui.page('/irrigation')
def irrigation_page():
    add_header()
    db = get_db()

    # 1. Fetch Field Options for the Dropdown
    # We need a list of names like ['Field A', 'Field B']
    all_fields = db.get_all_fields() # Returns objects
    field_options = [f.name for f in all_fields]
    # Create a lookup map {id: name} for the table display
    id_to_name = {f.id: f.name for f in all_fields}

    # 2. Define Custom Load Function
    # The DB returns objects with 'field_id'. The Table expects 'field_name'.
    def load_irrigation_data():
        events = db.query_irrigation_events() # Returns list of Irrigation objects
        rows = []
        for e in events:
            # Convert object to dict
            row = {
                'id': e.id,
                'date': e.date.isoformat() if e.date else None, # Convert date obj to string for UI
                'method': e.method,
                'amount': e.amount,
                'field_name': id_to_name.get(e.field_id, 'Unknown'), # Inject Name
            }
            rows.append(row)
        # Sort by date descending
        rows.sort(key=lambda x: x['date'] or '', reverse=True)
        return rows

    # --- THE BULK SAVE LOGIC ---
    def handle_save(**kwargs):
        """
        Intercepts the form data.
        kwargs['field_name'] might be a list ['Field A', 'Field B']
        """
        fields = kwargs.pop('field_name') # Remove from dict
        current_id = kwargs.get('id')
        
        # Ensure it's a list (just in case)
        if not isinstance(fields, list):
            fields = [fields]
            
        if not fields:
            raise ValueError("Please select at least one field")

        # CASE 1: EDIT MODE (ID exists)
        # If we are editing, we usually update the main row.
        # If the user selected MORE fields, we treat the extras as new additions.
        if current_id:
            # Update the original ID with the first selection
            first_field = fields[0]
            kwargs['field_name'] = first_field
            db.add_irrigation_event(**kwargs) # This updates the existing row
            
            # If there are others selected during edit, create NEW entries for them
            remaining_fields = fields[1:]
        else:
            # CASE 2: ADD MODE (No ID)
            # Create new entries for everything
            remaining_fields = fields

        # Loop through remaining and Create New (ID=None)
        for f in remaining_fields:
            # We must clear the ID so DB creates a new row
            entry_data = kwargs.copy()
            entry_data['id'] = None 
            entry_data['field_name'] = f
            db.add_irrigation_event(**entry_data)

    IRRIGATION_SCHEMA = [
        {'name': 'field_name', 'label': 'Anlage', 'type': 'select', 'options': field_options, 'required': True, 'multiple': True},
        {'name': 'date', 'label': 'Datum', 'type': 'date', 'required': True},
        {'name': 'method', 'label': 'Methode', 'type': 'text', 'required': True, 'default': 'Tropfer'},
        {'name': 'amount', 'label': 'Wassermenge [%]', 'type': 'number', 'default': 100},
    ]

    # 4. Instantiate
    TableEditor(
        title='Bewässerung',
        schema=IRRIGATION_SCHEMA,
        load_func=load_irrigation_data, 
        save_func=handle_save, 
        delete_func=db.delete_irrigation_event
    ).build_ui()