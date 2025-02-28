import os
from functools import lru_cache

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings


load_dotenv()


if (os.getenv("IMPORT_GCP_SECRET_MANAGER") or "").strip().lower() in ["true", "1", "y", "yes"]:
    from google.cloud import secretmanager_v1
    print("Imported GCP Secret Manager")
else:
    print("Not importing GCP Secret Manager")


class Config(BaseSettings):
    pulsar_host: str | None = Field(default=None)
    pulsar_audience: str | None = Field(default=None)
    pulsar_issuer_url: str | None = Field(default=None)
    pulsar_oauth_key_path: str | None = Field(default=None)


@lru_cache
def get_config() -> Config:
    return Config()
