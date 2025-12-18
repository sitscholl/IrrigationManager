from nicegui import ui

# Enhanced CSS with Glassmorphism and better animations
ui.add_head_html("""
<style>
    .glass-header {
        background: rgba(25, 118, 210, 0.85) !important; /* Primary color with transparency */
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    }

    .nav-link {
        color: rgba(255, 255, 255, 0.8);
        text-decoration: none;
        padding: 8px 12px;
        border-radius: 8px;
        transition: all 0.3s ease;
        font-weight: 500;
        display: flex;
        align-items: center;
        gap: 8px;
    }

    .nav-link:hover {
        color: white;
        background: rgba(255, 255, 255, 0.15);
    }

    .nav-active {
        color: white !important;
        background: rgba(255, 255, 255, 0.25) !important;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
    }

    /* Subtle pulse animation for icons on active links */
    .nav-active i {
        transform: scale(1.1);
    }
</style>
""", shared=True)

def add_header():
    menu_items = {
        'Dashboard': ('/', 'dashboard'),
        'Anlagen': ('/fields', 'agriculture'),
        'Bewässerung': ('/irrigation', 'water_drop'),
    }

    current_path = ui.context.client.page.path

    # --- THE HEADER ---
    with ui.header().classes('glass-header items-center px-6 h-16'):
        
        # Brand / Logo Section
        with ui.row().classes('items-center gap-2'):
            ui.icon('opacity', size='2rem').classes('text-white')
            ui.label('IrrigSmart').classes('text-xl font-bold text-white tracking-tight')

        ui.element('q-space') # Pushes nav to the right

        # Desktop Navigation (Hidden on small screens)
        with ui.row().classes('max-md:hidden gap-x-2'):
            for title, (path, icon) in menu_items.items():
                is_active = current_path == path
                
                with ui.link(target=path).classes(f'nav-link {"nav-active" if is_active else ""}'):
                    ui.icon(icon)
                    ui.label(title)

        # Mobile Menu Button (Shown only on small screens)
        with ui.button(icon='menu', color='white').props('flat round').classes('md:hidden'):
            with ui.menu().classes('w-48'):
                for title, (path, icon) in menu_items.items():
                    # Mobile menu items
                    ui.menu_item(title, on_click=lambda p=path: ui.navigate.to(p)) \
                        .classes('font-medium')

    # Optional: Side Drawer for a more "App-like" mobile feel 
    # (Uncomment below if you prefer a drawer over a simple menu)
    # left_drawer = ui.left_drawer(value=False).classes('bg-slate-50')
    # with left_drawer:
    #     ui.label('Navigation').classes('text-xs font-bold text-slate-400 p-4 uppercase')
    #     for title, (path, icon) in menu_items.items():
    #         ui.item(title, on_click=lambda p=path: ui.navigate.to(p)).classes('p-4')