"""Tests for settings module."""

from pydantic_ai_stream import settings


class TestSettings:
    def test_default_redis_prefix(self):
        assert settings.redis_prefix == "pyaix"

    def test_set_redis_prefix(self):
        original = settings.redis_prefix
        try:
            settings.set_redis_prefix("custom")
            assert settings.redis_prefix == "custom"
        finally:
            settings.set_redis_prefix(original)

    def test_set_redis_prefix_thread_safe(self):
        import threading

        results = []
        original = settings.redis_prefix

        def set_prefix(prefix):
            settings.set_redis_prefix(prefix)
            results.append(settings.redis_prefix)

        try:
            threads = [
                threading.Thread(target=set_prefix, args=(f"prefix-{i}",))
                for i in range(10)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            assert len(results) == 10
        finally:
            settings.set_redis_prefix(original)
