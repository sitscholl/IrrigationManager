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

    with ui.column().classes('w-full max-w-4xl mx-auto gap-3 items-stretch'):

        table = ui.table(
            title='Felder',
            columns=[
                {'name': 'name', 'label': 'Name', 'field': 'name'},
                {'name': 'reference_station', 'label': 'Station', 'field': 'reference_station'},
                {'name': 'soil_type', 'label': 'Boden', 'field': 'soil_type'},
                {'name': 'humus_pct', 'label': 'Humus %', 'field': 'humus_pct'},
                {'name': 'root_depth_cm', 'label': 'Wurzeltiefe cm', 'field': 'root_depth_cm'},
                {'name': 'area_ha', 'label': 'Fläche ha', 'field': 'area_ha'},
                {'name': 'p_allowable', 'label': 'p_allowable', 'field': 'p_allowable'},
                {'name': 'actions', 'label': '', 'field': 'id'},
            ],
            rows=_load_rows(db),
            row_key='id',
        )

        def refresh():
            table.rows = _load_rows(db)

        def submit():
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

        with ui.card().classes('mx-auto shadow-md').style('width: fit-content; max-width: 100%;'):
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
                    p_allow = ui.number('p_allowable (0–1)', value=0.4)
            ui.button('Speichern', on_click=submit)

        def delete(field_id: int):
            if db.delete_field(field_id):
                ui.notify('Gelöscht', color='green')
                refresh()
            else:
                ui.notify('Feld nicht gefunden', color='red')

        ui.separator()

        # with table.add_slot('delete'):
        #     for row in table.rows:
        #         with ui.td().props(f'key={row["id"]}'):
        #             for col in table.columns[:-1]:
        #                 ui.td(row[col['field']])
        #             with ui.td():
        #                 ui.button('Löschen', color='red', on_click=lambda r=row: delete(r['id']))