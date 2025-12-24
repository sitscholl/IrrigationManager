import asyncio
import logging
from datetime import datetime, time, timedelta
from typing import Callable, Awaitable

logger = logging.getLogger(__name__)

class IrrigationScheduler:
    def __init__(self, callback: Callable[[], Awaitable], time_of_day: str = "05:00"):
        self.time_of_day = self._parse_time_of_day(time_of_day)
        self.callback = callback
        self._task: asyncio.Task | None = None

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run_loop())
            logger.info(f"Scheduler started for daily run at {self.time_of_day.strftime('%H:%M')}")

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
            
            sleep_seconds = self._seconds_until_next_run()
            logger.info(f"Scheduler: Next run in {sleep_seconds}s")
            await asyncio.sleep(sleep_seconds)

    def _seconds_until_next_run(self) -> int:
        now = datetime.now()
        next_run = datetime.combine(now.date(), self.time_of_day)
        if next_run <= now:
            next_run = next_run + timedelta(days=1)
        return int((next_run - now).total_seconds())

    @staticmethod
    def _parse_time_of_day(value: str) -> time:
        try:
            parsed = datetime.strptime(value, "%H:%M").time()
        except ValueError as exc:
            raise ValueError("time_of_day must be in HH:MM 24-hour format (e.g., '05:00').") from exc
        return parsed
