import asyncio
import logging
import pandas as pd

from nicegui import ui

from ..config import load_config
from ..database.db import IrrigDB
from ..workflow import WaterBalanceWorkflow
from .deps import get_db
from .header import add_header

logger = logging.getLogger(__name__)
_fig_cache = None
_build_lock = asyncio.Lock()

def build_waterbalance_fig():
    cfg = load_config('config/config.yaml')
    db = IrrigDB(**cfg.get('database', {}))
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
        
async def get_latest_water_balance(fields, db):

    wb = []
    for field in fields:
        wb_field = db.latest_water_balance(field.id)
        if wb_field is not None:
            wb_field = pd.DataFrame(
                columns = ['Anlage', 'Datum', 'Wasserbilanz'],
                values =[wb_field.name, wb_field.date, wb_field.soil_storage]
            )
            wb.append(wb_field)

    if len(wb) > 0:
        return pd.concat(wb)
    else:
        logger.info("No water balance entries found")
        return pd.DataFrame(columns = ['Anlage', 'Datum', 'Wasserbilanz'])

@ui.page('/')
async def dashboard():
    add_header()
    db = get_db()
    fields = db.get_all_fields()
    water_balance = await get_latest_water_balance(fields, db)

    with ui.column().classes("w-full max-w-4xl mx-auto gap-3 items-stretch"):

        with ui.row().classes("w-full items-center"):
            ui.markdown('## Water balance')
            # ui.button(
            #     'Re-run workflow',
            #     on_click=lambda: asyncio.create_task(load_and_render(force=True)),
            # )

        container = ui.element().classes("w-full")

        async def load_and_render(force: bool = False):
            fig = await get_fig(force=force)
            # tighten figure height to reduce vertical whitespace
            # fig.update_layout(height=500)

            container.clear()
            # enter the slot explicitly to add new UI elements
            with container:
                ui.plotly(fig).classes('w-full')

        asyncio.create_task(load_and_render())

        ui.separator()

        ui.table.from_pandas(water_balance)
