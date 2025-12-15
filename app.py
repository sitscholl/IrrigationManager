import asyncio

import logging
import logging.config
from pathlib import Path
from importlib import import_module

from nicegui import ui, app

from src.config import load_config
from src.frontend.dashboard import get_fig
from src.frontend import deps #initialize startup/shutdow hooks

config = load_config('config/config.yaml')
logging.config.dictConfig(config['logging'])

for path in Path('src/frontend').glob('*.py'):
    if path.stem.startswith('_') or path.stem in {'__init__'}:
        continue
    import_module(f'src.frontend.{path.stem}')

async def refresh_loop(interval_seconds: int = 3600):
    while True:
        await get_fig(force=True)  # rebuild cache
        await asyncio.sleep(interval_seconds)

async def start_scheduler():
    # prevent multiple loops if reload is on
    if getattr(start_scheduler, '_started', False):
        return
    start_scheduler._started = True
    asyncio.create_task(refresh_loop())

app.on_startup(start_scheduler)
ui.run(
    title='Irrigation Manager',
    reload=True,
)