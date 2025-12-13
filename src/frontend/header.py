from nicegui import ui

def add_header() -> ui.button:
    """Create the page header."""
    menu_items = {
        'Dashboard': '/',
        'Test1': '/#test1',
        'Test2': '/#test2',
    }

    with ui.header() \
            .classes('items-center duration-200 p-0 px-4 no-wrap') \
            .style('box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1)'):

        with ui.row().classes('max-[1050px]:hidden'):
            for title_, target in menu_items.items():
                ui.link(title_, target).classes(replace='text-lg text-white')