from threading import Lock
from pydantic_settings import BaseSettings, SettingsConfigDict

lock = Lock()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="pydantic_ai_stream")

    redis_prefix: str = "pyaix"

    def set_redis_prefix(self, prefix: str):
        with lock:
            self.redis_prefix = prefix


settings = Settings()
