import asyncio
from functools import partial
from typing import Self

from pulsar import Client, Consumer, Message


class AsyncPulsarConsumer(Consumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._loop = asyncio.get_running_loop()

    async def areceive(self, *args, **kwargs) -> Message:
        return await self._loop.run_in_executor(None, partial(super().receive, *args, **kwargs))

    async def aacknowledge(self, *args, **kwargs) -> None:
        return await self._loop.run_in_executor(None, partial(super().acknowledge, *args, **kwargs))

    async def anegative_acknowledge(self, *args, **kwargs) -> None:
        return await self._loop.run_in_executor(
            None, partial(super().negative_acknowledge, *args, **kwargs)
        )

    @classmethod
    def from_consumer(cls, consumer: Consumer) -> Self:
        # Transfer the internal state from the regular consumer to our async one
        async_consumer = cls.__new__(cls)
        async_consumer.__dict__.update(consumer.__dict__)
        async_consumer._loop = asyncio.get_running_loop()
        return async_consumer


class AsyncPulsarClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._loop = asyncio.get_running_loop()

    async def asubscribe(self, *args, **kwargs) -> AsyncPulsarConsumer:
        consumer = await self._loop.run_in_executor(
            None, partial(super().subscribe, *args, **kwargs)
        )
        return AsyncPulsarConsumer.from_consumer(consumer)
