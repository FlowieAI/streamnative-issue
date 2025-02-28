import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from pulsar import AuthenticationOauth2
from pydantic import BaseModel, ConfigDict, PrivateAttr

from streamnative.configuration import Config
from streamnative.pulsar_service.consumer import TopicConsumer
from streamnative.pulsar_service.handler import MESSAGE_HANDLERS, BaseMessageHandler
from streamnative.pulsar_service.utils import AsyncPulsarClient

pulsar_logger = logging.getLogger("pulsar")
pulsar_logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
pulsar_logger.addHandler(handler)


def create_pulsar_client(config: Config, **kwargs) -> AsyncPulsarClient:
    oauth_key_path = Path(config.pulsar_oauth_key_path)
    if not oauth_key_path.exists():
        raise FileNotFoundError(f"OAuth key file not found at {oauth_key_path}")

    oauth_config = {
        "issuer_url": config.pulsar_issuer_url,
        "private_key": str(oauth_key_path.absolute()),
        "audience": config.pulsar_audience,
    }
    auth = AuthenticationOauth2(json.dumps(oauth_config))
    return AsyncPulsarClient(
        config.pulsar_host, authentication=auth, logger=pulsar_logger, **kwargs
    )


class PulsarService(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    config: Config
    message_handlers: list[type[BaseMessageHandler]]

    _pulsar_client: AsyncPulsarClient | None = PrivateAttr(default=None)

    @property
    def pulsar_client(self) -> AsyncPulsarClient:
        if self._pulsar_client is None:
            print("Creating Pulsar client")
            self._pulsar_client = create_pulsar_client(self.config)
        return self._pulsar_client

    async def run(self, shutdown_event: asyncio.Event) -> None:
        try:
            async with self._create_pulsar_consumers(shutdown_event) as consumers:
                await asyncio.gather(*(consumer.start() for consumer in consumers))
        finally:
            if self._pulsar_client:
                print("Closing Pulsar client")
                self._pulsar_client.close()
                self._pulsar_client = None

    @asynccontextmanager
    async def _create_pulsar_consumers(
        self, shutdown_event: asyncio.Event
    ) -> AsyncIterator[list[TopicConsumer]]:
        try:
            consumers = []
            for handler_cls in self.message_handlers:
                handler = handler_cls()

                consumer = TopicConsumer(
                    client=self.pulsar_client,
                    shutdown_event=shutdown_event,
                    consumer_config=handler.consumer_config,
                    message_handler=handler,
                )
                consumers.append(consumer)

            yield consumers
        except Exception as e:
            print(f"Error creating Pulsar consumers: {e}")
            raise


def create_pulsar_service(config: Config) -> PulsarService:
    return PulsarService(
        config=config,
        message_handlers=list(MESSAGE_HANDLERS),
    )
