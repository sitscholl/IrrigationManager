import asyncio
import logging

from nicegui import ui

from ..config import load_config
from ..database.db import IrrigDB
from ..workflow import WaterBalanceWorkflow

from .header import add_header

logger = logging.getLogger(__name__)
_fig_cache = None
_build_lock = asyncio.Lock()

def build_waterbalance_fig():
    cfg = load_config('config/config.yaml')
    db = IrrigDB(**cfg.get('database', {}))
    db.load_fields_from_config(cfg.get('fields_config', 'config/fields.yaml'))
    wf = WaterBalanceWorkflow(cfg, db)
    wf.run()
    return wf.plot.fig


async def get_fig(force: bool = False):
    """
    Get (or rebuild) the figure. A lock prevents concurrent rebuilds.
    """
    global _fig_cache
    if _fig_cache is not None and not force:
        return _fig_cache

    async with _build_lock:
        if _fig_cache is not None and not force:
            return _fig_cache
        _fig_cache = await asyncio.to_thread(build_waterbalance_fig)
        return _fig_cache


@ui.page('/')
async def dashboard():
    add_header()
    
    with ui.column().classes("w-full max-w-4xl mx-auto gap-3 items-stretch"):

        with ui.row().classes("w-full items-center justify-center"):
            ui.markdown('## Water balance')
            # ui.button(
            #     'Re-run workflow',
            #     on_click=lambda: asyncio.create_task(load_and_render(force=True)),
            # )

        with ui.element().classes('flex-grow'):
            container = ui.element().classes("w-full")
            status = ui.label('Loading data...')
            spinner = ui.spinner('bars', size='6em')

            async def load_and_render(force: bool = False):
                fig = await get_fig(force=force)
                # tighten figure height to reduce vertical whitespace
                #fig.update_layout(height=500)

                status.delete()
                spinner.delete()
                container.clear()
                # enter the slot explicitly to add new UI elements
                with container:
                    ui.plotly(fig).classes('w-full')

        asyncio.create_task(load_and_render())
