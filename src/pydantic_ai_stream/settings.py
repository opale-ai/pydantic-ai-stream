from threading import Lock
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="pydantic_ai_stream")

    lock: Lock = Field(default_factory=Lock)
    redis_prefix: str = "pyaix"

    def set_redis_prefix(self, prefix: str):
        with self.lock:
            self.redis_prefix = prefix


settings = Settings()
