from nicegui import ui


# Add CSS once
ui.add_head_html("""
<style>
    /* Base header link styling */
    .header-link {
        position: relative;
        color: white;
        text-decoration: none;
        display: flex;
        align-items: center;
        gap: 6px;
        transition: color 0.25s ease;
    }

    .header-link:hover {
        color: #a0c4ff;
    }

    /* Hover underline animation */
    .header-link::after {
        content: "";
        position: absolute;
        left: 0;
        bottom: -4px;
        width: 100%;
        height: 2px;
        background-color: #a0c4ff;
        transform: scaleX(0);
        transform-origin: left;
        transition: transform 0.25s ease;
    }
    .header-link:hover::after {
        transform: scaleX(1);
    }

    /* ACTIVE PAGE STYLE */
    .active-link {
        color: #a0c4ff !important;
        font-weight: 600;
    }

    /* Persistent underline for active page */
    .active-link::after {
        transform: scaleX(1) !important;
        background-color: #a0c4ff;
    }
</style>
""", shared = True)


def add_header() -> ui.button:
    """Create the page header."""

    menu_items = {
        'Dashboard': ('/', 'dashboard'),
        'Anlagen': ('/fields', 'agriculture'),
        'Bewässerung': ('/irrigation', 'water_drop'),
    }

    current = ui.context.client.page.path  # <-- detects the current page

    with ui.header().classes(
        'items-center justify-center duration-200 p-0 px-4 no-wrap h-10'
    ).style('box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1)'):

        with ui.row().classes('max-[1050px]:hidden gap-x-10'):
            for title_, (target, icon) in menu_items.items():

                # Determine if this link is the active route
                link_classes = 'header-link text-lg'
                if current == target:
                    link_classes += ' active-link'

                # Build link with icon + label
                with ui.link(target=target).classes(link_classes):
                    ui.icon(icon).classes('text-lg')
                    ui.label(title_)
