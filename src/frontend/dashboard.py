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
    # Use a context manager if your IrrigDB supports it to ensure connection closure
    cfg = load_config('config/config.yaml')
    db = IrrigDB(**cfg.get('database', {}))
    wf = WaterBalanceWorkflow(cfg, db)
    wf.run()
    # Apply aesthetic tweaks to plotly figure directly
    fig = wf.plot.fig
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        modebar_remove=['lasso2d', 'select2d']
    )
    return fig

async def get_fig(force: bool = False):
    global _fig_cache
    if _fig_cache is not None and not force:
        return _fig_cache

    async with _build_lock:
        if _fig_cache is not None and not force:
            return _fig_cache
        _fig_cache = await asyncio.to_thread(build_waterbalance_fig)
        return _fig_cache

async def get_latest_water_balance(fields, db):
    data = []
    for field in fields:
        wb_field = db.latest_water_balance(field.id)
        if wb_field:
            data.append({
                'Anlage': field.name,
                'Datum': wb_field.date.strftime('%Y-%m-%d') if wb_field.date else '',
                'Wasserbilanz': f"{wb_field.soil_storage:.2f} mm"
            })

    return pd.DataFrame(data) if data else pd.DataFrame(columns=['Anlage', 'Datum', 'Wasserbilanz'])

@ui.refreshable
async def render_dashboard_content(db, fields, force: bool = False):
    """This function contains the parts of the UI that need to change when data refreshes."""
    
    # 1. Start fetching data (Table and Chart)
    # We can run these in parallel to save time
    df_task = get_latest_water_balance(fields, db)
    fig_task = get_fig(force=force)
    
    # Show a loading placeholder while waiting
    with ui.column().classes('w-full items-center q-pa-xl') as placeholder:
        ui.spinner(size='lg')
        ui.label('Updating data...')

    df_balance, fig = await asyncio.gather(df_task, fig_task)
    placeholder.delete() # Remove the spinner

    # 2. Render Chart Card
    with ui.card().classes("w-full shadow-lg rounded-xl overflow-hidden"):
        ui.label('Water Balance Trend').classes("text-lg font-semibold q-pa-md")
        ui.plotly(fig).classes('w-full h-[500px]')

    # 3. Render Table Card
    with ui.card().classes("w-full shadow-lg rounded-xl q-pa-none"):
        ui.label('Latest Readings').classes("text-lg font-semibold q-pa-md")
        if not df_balance.empty:
            ui.table.from_pandas(df_balance).classes("w-full").props('flat bordered')
        else:
            ui.label('No data available').classes('q-pa-md text-italic')

@ui.page('/')
async def dashboard():
    add_header()
    db = get_db()
    fields = db.get_all_fields()
    
    with ui.column().classes("w-full max-w-5xl mx-auto q-pa-md gap-6"):
        # Header Section (Static)
        with ui.row().classes("w-full justify-between items-center"):
            with ui.column():
                ui.label('Field Overview').classes("text-3xl font-bold text-slate-800")
                ui.label('Real-time water balance monitoring').classes("text-slate-500")
            
            # The button now just triggers the refreshable function
            ui.button('Refresh Data', icon='refresh', 
                      on_click=lambda: render_dashboard_content.refresh(force=True)) \
                .props('outline')

        # Dynamic Section (Chart and Table)
        await render_dashboard_content(db, fields)