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


def build_field_form():
    with ui.row().classes('w-full justify-center items-start gap-4 flex-wrap') as container:
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
    return {
        'container': container,
        'name': name,
        'ref': ref,
        'soil': soil,
        'humus': humus,
        'root': root,
        'area': area,
        'p_allow': p_allow,
    }


def read_form(frm):
    return dict(
        name=frm['name'].value,
        reference_station=frm['ref'].value,
        soil_type=frm['soil'].value,
        humus_pct=frm['humus'].value or 0,
        root_depth_cm=frm['root'].value or 30,
        area_ha=frm['area'].value,
        p_allowable=frm['p_allow'].value or 0,
    )


@ui.page('/fields')
def fields():
    add_header()
    db = get_db()
    selected = {'row': None}

    def refresh():
        table.rows = _load_rows(db)

    def require_selection():
        sel = table.selected
        if not sel:
            ui.notify('Bitte zuerst ein Feld auswaehlen', color='red')
            return None
        row = sel[0] if isinstance(sel, list) else sel
        selected['row'] = row
        return row

    # --- Add ---
    def add_field():
        data = read_form(add_form)
        if not data['name'] or not data['reference_station'] or not data['soil_type']:
            ui.notify('Name, Station und Bodenart sind Pflichtfelder', color='red')
            return
        db.add_field(**data)
        ui.notify('Gespeichert', color='green')
        refresh()
        add_dialog.close()

    # --- Edit ---
    def open_edit():
        row = require_selection()
        if not row:
            return
        edit_form['name'].value = row.get('name')
        edit_form['ref'].value = row.get('reference_station')
        edit_form['soil'].value = row.get('soil_type')
        edit_form['humus'].value = row.get('humus_pct')
        edit_form['root'].value = row.get('root_depth_cm')
        edit_form['area'].value = row.get('area_ha')
        edit_form['p_allow'].value = row.get('p_allowable')
        edit_dialog.open()

    def edit_field():
        row = selected['row']
        if not row:
            ui.notify('Kein Feld ausgewaehlt', color='red')
            return
        data = read_form(edit_form)
        db.add_field(**data)
        ui.notify('Aktualisiert', color='green')
        refresh()
        edit_dialog.close()

    # --- Delete ---
    def open_delete():
        row = require_selection()
        if not row:
            return
        remove_label.text = f"Soll \"{row.get('name', '')}\" geloescht werden?"
        delete_dialog.open()

    def delete_field():
        row = selected['row']
        if not row:
            ui.notify('Kein Feld ausgewaehlt', color='red')
            return
        if db.delete_field(row['id']):
            ui.notify('Geloescht', color='green')
            refresh()
        else:
            ui.notify('Feld nicht gefunden', color='red')
        delete_dialog.close()
        selected['row'] = None

    # Dialogs
    with ui.dialog() as edit_dialog, ui.card().classes("m-auto"):
        ui.markdown('## Feld bearbeiten')
        edit_form = build_field_form()
        ui.button('Speichern', on_click=edit_field)

    with ui.dialog() as add_dialog, ui.card().classes("m-auto"):
        ui.markdown('## Feld hinzufuegen')
        add_form = build_field_form()
        ui.button('Speichern', on_click=add_field)

    with ui.dialog() as delete_dialog, ui.card().classes("m-auto items-stretch"):
        ui.markdown('## Feld loeschen')
        remove_label = ui.label()
        with ui.row().classes('justify-center'):
            ui.button('Ja', on_click=delete_field)
            ui.button('Nein', on_click=delete_dialog.close)

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
            ui.button('Edit', on_click=open_edit).props('color=orange').classes('col')
            ui.button('Remove', on_click=open_delete).props('color=red').classes('col')
