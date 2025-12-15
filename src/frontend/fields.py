from nicegui import ui

from .header import add_header
from .deps import get_db


def _load_rows(db):
    return [
        {
            'id': f.id,
            'name': f.name,
            'reference_station': f.reference_station,
            'soil_type': f.soil_type,
            'humus_pct': f.humus_pct,
            'root_depth_cm': f.root_depth_cm,
            'area_ha': f.area_ha,
            'p_allowable': f.p_allowable,
        }
        for f in db.get_all_fields()
    ]


@ui.page('/fields')
def fields():
    add_header()
    db = get_db()

    selected_field_id: int | None = None

    def refresh():
        table.rows = _load_rows(db)

    def add_field():
        if not name.value or not ref.value or not soil.value:
            ui.notify('Name, Station und Bodenart sind Pflichtfelder', color='red')
            return
        db.add_field(
            name=name.value,
            reference_station=ref.value,
            soil_type=soil.value,
            humus_pct=humus.value or 0,
            root_depth_cm=root.value or 30,
            area_ha=area.value,
            p_allowable=p_allow.value or 0,
        )
        ui.notify('Gespeichert', color='green')
        refresh()
        add_dialog.close()

    def ask_delete():
        nonlocal selected_field_id
        selection = table.selected
        if not selection:
            ui.notify('Bitte zuerst ein Feld auswählen', color='red')
            return
        selected = selection[0] if isinstance(selection, list) else selection
        selected_field_id = selected.get('id')
        remove_label.text = f"Soll \"{selected.get('name', '')}\" gelöscht werden?"
        remove_dialog.open()

    def delete_selected():
        nonlocal selected_field_id
        if selected_field_id is None:
            ui.notify('Kein Feld ausgewählt', color='red')
            return
        if db.delete_field(selected_field_id):
            ui.notify('Gelöscht', color='green')
            refresh()
        else:
            ui.notify('Feld nicht gefunden', color='red')
        selected_field_id = None
        remove_dialog.close()

    # Dialog: add field
    with ui.dialog() as add_dialog, ui.card().classes('m-auto'):
        ui.markdown('## Feld hinzufügen')
        with ui.row().classes('w-full justify-center items-start gap-4 flex-wrap'):
            with ui.column().classes('min-w-[220px] gap-2'):
                name = ui.input('Name')
                ref = ui.input('Reference station')
                soil = ui.input('Soil type')
            with ui.column().classes('min-w-[220px] gap-2'):
                humus = ui.number('Humus %', value=2.0)
                root = ui.number('Root depth (cm)', value=30)
                area = ui.number('Area (ha)', value=None)
            with ui.column().classes('min-w-[220px] gap-2'):
                p_allow = ui.number('p_allowable (0-1)', value=0.4)
        with ui.row():
            ui.button('Submit', on_click=add_field).props('color=green')
            ui.button('Close', on_click=add_dialog.close).props('color=red')

    # Dialog: remove field
    with ui.dialog() as remove_dialog, ui.card().classes('m-auto'):
        remove_label = ui.label().classes('m-auto')
        with ui.row().classes('m-auto row').style('width: 100%;'):
            ui.button('Yes', on_click=delete_selected).props('color=green').classes('col')
            ui.button('No', on_click=remove_dialog.close).props('color=red').classes('col')

    # Main card and table
    with ui.column().classes('w-full max-w-4xl mx-auto gap-3 items-stretch'):
        ui.markdown('## Anlagen')
        table = ui.table(
            columns=[
                {'name': 'name', 'label': 'Name', 'field': 'name'},
                {'name': 'reference_station', 'label': 'Station', 'field': 'reference_station'},
                {'name': 'soil_type', 'label': 'Boden', 'field': 'soil_type'},
                {'name': 'humus_pct', 'label': 'Humus %', 'field': 'humus_pct'},
                {'name': 'root_depth_cm', 'label': 'Wurzeltiefe cm', 'field': 'root_depth_cm'},
                {'name': 'area_ha', 'label': 'Flaeche ha', 'field': 'area_ha'},
                {'name': 'p_allowable', 'label': 'p_allowable', 'field': 'p_allowable'},
                {'name': 'actions', 'label': '', 'field': 'id'},
            ],
            rows=_load_rows(db),
            row_key='id',
            selection='single',
        )

        with ui.row().classes('m-auto row').style('width: 100%;'):
            ui.button('Add', on_click=add_dialog.open).props('color=green').classes('col')
            ui.button('Remove', on_click=ask_delete).props('color=red').classes('col')
