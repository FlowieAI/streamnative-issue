import asyncio
import contextlib
import signal

from streamnative.configuration import get_config
from streamnative.pulsar_service import create_pulsar_service


async def main():
    config = get_config()

    try:
        shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: shutdown_event.set())

        pulsar_service = create_pulsar_service(config)

        pulsar_task = asyncio.create_task(pulsar_service.run(shutdown_event))
        shutdown_task = asyncio.create_task(shutdown_event.wait())

        tasks = [pulsar_task, shutdown_task]

        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait([shutdown_task], return_when=asyncio.FIRST_COMPLETED)

        await asyncio.sleep(2)  # Give time for shutdown event to propagate
        for task in tasks:
            if not task.done():
                task.cancel()

        # Wait for all tasks to complete their cleanup after cancellation
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception:
        raise


if __name__ == "__main__":
    asyncio.run(main())
