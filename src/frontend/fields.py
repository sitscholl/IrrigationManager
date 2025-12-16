from nicegui import ui
from .header import add_header
from .deps import get_db
# Import the reusable class
from .table_editor import TableEditor 

# Define Schema
FIELD_SCHEMA = [
    {'name': 'name', 'label': 'Name', 'type': 'text', 'required': True},
    {'name': 'reference_station', 'label': 'Station', 'type': 'text', 'required': True},
    {'name': 'soil_type', 'label': 'Soil Type', 'type': 'text', 'required': True},
    {'name': 'humus_pct', 'label': 'Humus %', 'type': 'number', 'default': 2.0},
    {'name': 'root_depth_cm', 'label': 'Root Depth (cm)', 'type': 'number', 'default': 30},
    {'name': 'area_ha', 'label': 'Area (ha)', 'type': 'number', 'default': 0},
    {'name': 'p_allowable', 'label': 'p_allowable', 'type': 'number', 'default': 0.4},
]

@ui.page('/fields')
def fields_page():
    add_header()
    db = get_db()

    # Instantiate the generic editor
    editor = TableEditor(
        title='Fields',
        schema=FIELD_SCHEMA,
        load_func=db.get_all_fields,
        save_func=db.add_field,
        delete_func=db.delete_field
    )
    
    # Render it
    editor.build_ui()