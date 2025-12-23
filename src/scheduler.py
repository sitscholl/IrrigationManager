import asyncio
import logging
from typing import Callable, Awaitable

logger = logging.getLogger(__name__)

class IrrigationScheduler:
    def __init__(self, callback: Callable[[], Awaitable], seconds: int = 0, minutes: int = 1, hours: int = 0):
        self.interval = seconds + minutes * 60 + hours * 3600
        if self.interval <= 0:
            raise ValueError('Interval must be greater than 0.')
        
        self.callback = callback
        self._task: asyncio.Task | None = None

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run_loop())
            logger.info(f"Scheduler started with interval {self.interval}s")

    def stop(self):
        if self._task:
            self._task.cancel()
            self._task = None

    async def _run_loop(self):
        while True:
            try:
                logger.info("Scheduler: Triggering scheduled update...")
                await self.callback()
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
            
            await asyncio.sleep(self.interval)