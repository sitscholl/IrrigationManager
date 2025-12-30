import logging
import logging.config
from pathlib import Path
from importlib import import_module

from nicegui import ui, app

from src.config import load_config
from src.frontend.dashboard import get_fig
from src.frontend import deps #initialize startup/shutdow hooks
from src.scheduler import IrrigationScheduler

config = load_config('config/config.yaml')
logging.config.dictConfig(config['logging'])

for path in Path('src/frontend').glob('*.py'):
    if path.stem.startswith('_') or path.stem in {'__init__'}:
        continue
    import_module(f'src.frontend.{path.stem}')

# 1. Define the callback (wrapped to ensure force=True)
async def scheduled_refresh():
    await get_fig(force=True)

# 2. Instantiate the scheduler (e.g., every 1 hour)
scheduler = IrrigationScheduler(callback=scheduled_refresh, **config.get('scheduler', {}))

# 3. Use standard NiceGUI lifecycle hooks
app.on_startup(lambda: scheduler.start())
app.on_shutdown(lambda: scheduler.stop())

ui.run(title='Irrigation Manager', host='0.0.0.0', port=8080, reload=False)
