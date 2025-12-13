import asyncio
import logging, logging.config
from nicegui import ui

from src.config import load_config
from src.database.db import IrrigDB
from src.workflow import WaterBalanceWorkflow

config = load_config('config/config.yaml')
logging.config.dictConfig(config['logging'])

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
async def index():
    ui.label('Water balance').classes('text-2xl font-semibold')
    status = ui.label('Loading data...')
    spinner = ui.spinner('bars', size='6em')
    container = ui.column()  # holds the plot once ready

    async def load_and_render(force: bool = False):
        fig = await get_fig(force=force)
        status.set_text('Ready')
        spinner.visible = False
        container.clear()
        # enter the slot explicitly to add new UI elements
        with container:
            ui.plotly(fig).classes('w-full h-[80vh]')

    asyncio.create_task(load_and_render())

    ui.button(
        'Re-run workflow',
        on_click=lambda: asyncio.create_task(load_and_render(force=True)),
    )

if __name__ == '__main__':
    ui.run(title='Irrigation Manager', reload=False)
