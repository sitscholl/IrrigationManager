from functools import lru_cache
from nicegui import app
from ..config import load_config
from ..database.db import IrrigDB

@lru_cache(maxsize=1)
def get_db() -> IrrigDB:
    cfg = load_config('config/config.yaml')
    return IrrigDB(**cfg.get('database', {}))

async def close_db():
    db = get_db()
    db.close()

app.on_shutdown(close_db())