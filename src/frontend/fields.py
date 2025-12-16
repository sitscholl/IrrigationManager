from nicegui import ui
from .header import add_header
from .deps import get_db

# 1. Configuration: Single Source of Truth
# Define your fields once. We use this to generate columns AND form inputs.
FIELD_SCHEMA = [
    {'name': 'name', 'label': 'Name', 'type': 'text', 'required': True},
    {'name': 'reference_station', 'label': 'Station', 'type': 'text', 'required': True},
    {'name': 'soil_type', 'label': 'Soil Type', 'type': 'text', 'required': True},
    {'name': 'humus_pct', 'label': 'Humus %', 'type': 'number', 'default': 2.0},
    {'name': 'root_depth_cm', 'label': 'Root Depth (cm)', 'type': 'number', 'default': 30},
    {'name': 'area_ha', 'label': 'Area (ha)', 'type': 'number', 'default': None},
    {'name': 'p_allowable', 'label': 'p_allowable (0-1)', 'type': 'number', 'default': 0.4},
]

@ui.page('/fields')
def fields():
    add_header()
    db = get_db()
    
    # State for the currently selected row (for logic) and form data (for binding)
    state = {
        'selected_row': None,
        'form_data': {} 
    }

    def load_data():
        """Convert DB objects to dicts for the table"""
        # Assuming db.get_all_fields returns objects with attributes matching schema 'name'
        rows = []
        for f in db.get_all_fields():
            # Dynamically build dict based on schema + always include ID
            row = {'id': f.id}
            for field in FIELD_SCHEMA:
                row[field['name']] = getattr(f, field['name'])
            rows.append(row)
        table.rows = rows
        table.update()

    def reset_form():
        """Clear form or set defaults defined in schema"""
        state['form_data'] = {}
        for field in FIELD_SCHEMA:
            state['form_data'][field['name']] = field.get('default', None)
        # ID is None for new entries
        state['form_data']['id'] = None 

    def open_dialog(is_edit=False):
        if is_edit:
            selected = table.selected
            if not selected:
                ui.notify('Please select a field first', color='red')
                return
            # Clone selected row data into form_data to avoid modifying table directly
            state['form_data'] = dict(selected[0])
            dialog_label.text = f"Edit Field: {state['form_data']['name']}"
        else:
            reset_form()
            dialog_label.text = "Add New Field"
            
        dialog.open()

    def save_field():
        data = state['form_data']
        
        # 1. Validation Loop
        for field in FIELD_SCHEMA:
            if field.get('required') and not data.get(field['name']):
                ui.notify(f"{field['label']} is required", color='red')
                return

        # 2. Save Logic
        try:
            # Check if this is an Update (has ID) or Create (no ID)
            if data.get('id'):
                # You might need to update your DB method to support update vs add
                # Assuming db.add_field handles upsert or you have db.update_field
                # db.update_field(**data) 
                db.add_field(**data) # Keeping your original call
                action = "Updated"
            else:
                db.add_field(**data)
                action = "Created"

            ui.notify(f'Field {action}', color='green')
            load_data()
            dialog.close()
        except Exception as e:
            ui.notify(f'Error: {str(e)}', color='red')

    def delete_field():
        selected = table.selected
        if not selected:
            ui.notify('No field selected', color='warning')
            return
            
        row = selected[0]
        # Confirm dialog logic could go here, keeping it simple for brevity
        if db.delete_field(row['id']):
            ui.notify(f"Deleted {row['name']}", color='green')
            load_data()
        else:
            ui.notify('Delete failed', color='red')

    # --- UI LAYOUT ---

    with ui.column().classes('w-full max-w-4xl mx-auto gap-3'):
        ui.markdown('## Anlagen')

        # 1. Dynamic Table Column Generation
        cols = [{'name': f['name'], 'label': f['label'], 'field': f['name']} for f in FIELD_SCHEMA]
        
        table = ui.table(
            columns=cols, 
            rows=[], 
            row_key='id', 
            selection='single'
        ).classes('w-full')

        # Buttons
        with ui.row():
            ui.button('Add', on_click=lambda: open_dialog(is_edit=False)).props('color=green')
            ui.button('Edit', on_click=lambda: open_dialog(is_edit=True)).props('color=orange')
            ui.button('Delete', on_click=delete_field).props('color=red')

    # --- SHARED DIALOG (Add & Edit) ---
    with ui.dialog() as dialog, ui.card().classes('min-w-[400px]'):
        dialog_label = ui.label('Field Details').classes('text-xl font-bold mb-4')
        
        # Generate Form Inputs dynamically
        with ui.column().classes('w-full gap-2'):
            for field in FIELD_SCHEMA:
                if field['type'] == 'number':
                    ui.number(field['label']).bind_value(state['form_data'], field['name']).classes('w-full')
                else:
                    ui.input(field['label']).bind_value(state['form_data'], field['name']).classes('w-full')
        
        with ui.row().classes('w-full justify-end mt-4'):
            ui.button('Cancel', on_click=dialog.close).props('flat')
            ui.button('Save', on_click=save_field)

    # Initial Load
    load_data()