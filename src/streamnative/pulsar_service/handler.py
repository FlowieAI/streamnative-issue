from abc import ABC, abstractmethod
from typing import Any, ClassVar, Literal

from pydantic import BaseModel


class ConsumerConfig(BaseModel):
    topics: list[str]
    subscription_name: str
    initial_position: Literal["Earliest", "Latest"]


class BaseMessageHandler(BaseModel, ABC):
    name: ClassVar[str]
    topics: ClassVar[list[str]]
    subscription_name: ClassVar[str]
    initial_position: ClassVar[Literal["Earliest", "Latest"]]

    @property
    def consumer_config(self) -> ConsumerConfig:
        values = {
            "topics": self.topics,
            "subscription_name": self.subscription_name,
            "initial_position": self.initial_position,
        }
        return ConsumerConfig(**values)

    @abstractmethod
    async def process(self, data: dict[str, Any]) -> None:
        pass


class DummyMsgHandler(BaseMessageHandler):
    name = "dummy"
    topics = [
        "flowie/analytics/simonetest",
    ]
    subscription_name = "streamnative.test1"
    initial_position = "Earliest"

    async def process(self, data: dict[str, Any]) -> None:
        try:
            print(f"Processing message: {data}")
        except Exception as e:
            print(f"Malformed message: {e}")
            return


MESSAGE_HANDLERS = {
    DummyMsgHandler,
}
