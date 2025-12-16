from nicegui import ui

class TableEditor:
    def __init__(self, title, schema, load_func, save_func, delete_func):
        """
        :param title: Header text for the page/section
        :param schema: List of dicts defining fields (name, label, type, required, etc.)
        :param load_func: Function returning a list of row objects/dicts
        :param save_func: Function accepting **kwargs to save/update data
        :param delete_func: Function accepting an ID to delete a row
        """
        self.title = title
        self.schema = schema
        self.load_func = load_func
        self.save_func = save_func
        self.delete_func = delete_func
        
        # State
        self.form_data = {}
        self.table = None
        self.dialog = None

    def build_ui(self):
        """Call this method to render the UI components"""
        
        with ui.column().classes('w-full max-w-4xl mx-auto gap-3'):
            ui.markdown(f'## {self.title}')

            # --- Table ---
            cols = [
                {'name': f['name'], 'label': f['label'], 'field': f['name'], 'sortable': True} 
                for f in self.schema
            ]
            
            self.table = ui.table(
                columns=cols, 
                rows=[], 
                row_key='id', 
                selection='single'
            ).classes('w-full')

            # --- Action Buttons ---
            with ui.row():
                ui.button('Add', on_click=self.open_add_dialog).props('color=green icon=add')
                ui.button('Edit', on_click=self.open_edit_dialog).props('color=orange icon=edit')
                ui.button('Delete', on_click=self.delete_selected).props('color=red icon=delete')

        # --- Dialog (Hidden by default) ---
        with ui.dialog() as self.dialog, ui.card().classes('min-w-[400px]'):
            self.dialog_label = ui.label('').classes('text-xl font-bold mb-4')
            
            with ui.column().classes('w-full gap-2'):
                for field in self.schema:
                    # Determine input type
                    if field.get('type') == 'number':
                        ui.number(field['label']).bind_value(self.form_data, field['name']).classes('w-full')
                    elif field.get('type') == 'checkbox':
                        ui.checkbox(field['label']).bind_value(self.form_data, field['name'])
                    else:
                        ui.input(field['label']).bind_value(self.form_data, field['name']).classes('w-full')

            with ui.row().classes('w-full justify-end mt-4'):
                ui.button('Cancel', on_click=self.dialog.close).props('flat')
                ui.button('Save', on_click=self.save_data)

        # Initial data load
        self.refresh_table()

    def refresh_table(self):
        """Fetch data from DB and convert to flat dicts for the table"""
        raw_rows = self.load_func()
        clean_rows = []
        for r in raw_rows:
            # Handle if DB returns objects or dicts
            row_dict = r if isinstance(r, dict) else r.__dict__
            # Ensure we only extract schema fields + id
            clean = {'id': row_dict.get('id')}
            for field in self.schema:
                clean[field['name']] = row_dict.get(field['name'])
            clean_rows.append(clean)
        
        self.table.rows = clean_rows
        self.table.update()

    def _reset_form(self, defaults=None):
        self.form_data.clear()
        # Always track ID (None for new, set for edit)
        self.form_data['id'] = None
        for field in self.schema:
            val = defaults.get(field['name']) if defaults else field.get('default')
            self.form_data[field['name']] = val

    def open_add_dialog(self):
        self._reset_form()
        self.dialog_label.text = f'Add {self.title}'
        self.dialog.open()

    def open_edit_dialog(self):
        selected = self.table.selected
        if not selected:
            ui.notify('Please select a row', color='warning')
            return
        
        # Load selected data into form
        row = selected[0]
        self._reset_form(defaults=row)
        self.form_data['id'] = row['id'] # Ensure ID is carried over
        
        self.dialog_label.text = f'Edit {self.title}'
        self.dialog.open()

    def save_data(self):
        # Validation
        for field in self.schema:
            if field.get('required') and not self.form_data.get(field['name']):
                ui.notify(f"{field['label']} is required", color='red')
                return

        try:
            # Call the provided save callback
            self.save_func(**self.form_data)
            ui.notify('Saved successfully', color='green')
            self.refresh_table()
            self.dialog.close()
        except Exception as e:
            ui.notify(f'Error: {str(e)}', color='red')

    def delete_selected(self):
        selected = self.table.selected
        if not selected:
            ui.notify('Please select a row', color='warning')
            return
        
        row_id = selected[0]['id']
        if self.delete_func(row_id):
            ui.notify('Deleted', color='green')
            self.refresh_table()
            self.table.selected = [] # Clear selection
        else:
            ui.notify('Delete failed', color='red')