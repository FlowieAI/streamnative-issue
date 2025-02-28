import asyncio
import json
from contextlib import suppress

import pulsar
from pydantic import BaseModel, ConfigDict, PrivateAttr

from streamnative.pulsar_service.handler import BaseMessageHandler, ConsumerConfig
from streamnative.pulsar_service.utils import AsyncPulsarClient, AsyncPulsarConsumer


class TopicConsumer(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    client: AsyncPulsarClient
    shutdown_event: asyncio.Event
    message_handler: BaseMessageHandler
    consumer_config: ConsumerConfig

    _consumer: AsyncPulsarConsumer = PrivateAttr()

    async def start(self) -> None:
        try:
            initial_position = getattr(
                pulsar.InitialPosition, self.consumer_config.initial_position
            )
            print(f"Subscribing to topic: {self.consumer_config.topics}")
            self._consumer = await self.client.asubscribe(
                topic=self.consumer_config.topics,
                subscription_name=self.consumer_config.subscription_name,
                initial_position=initial_position,
                consumer_type=pulsar.ConsumerType.Shared,
                receiver_queue_size=1,
            )
            print(f"Pulsar consumer created: {self._consumer.topic()}")

            await self._consume_messages()
        finally:
            await self.stop()

    async def _consume_messages(self):
        print("Starting message consumption")
        while True:
            if self.shutdown_event.is_set():
                print(f"Consumer shutdown requested: {self.consumer_config.topics}")
                break
            try:
                msg = await self._consumer.areceive(timeout_millis=1000)
            except pulsar.Timeout:
                continue
            except Exception as e:
                print(f"Error receiving message: {e}")
                continue

            try:
                data = json.loads(msg.data().decode("utf-8"))
                print(f"Received message: {data}")
                await self.message_handler.process(data)
                await self._consumer.aacknowledge(msg)
            except Exception as e:
                print(f"Error processing message: {e}")
                await self._consumer.anegative_acknowledge(msg)

    async def stop(self):
        with suppress(Exception):
            if self._consumer:
                print(f"Stopping consumer: {self.consumer_config.topics}")
                self._consumer.close()
