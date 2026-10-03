import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    mqtt_host: str = field(default_factory=lambda: _env("MQTT_HOST", "localhost"))
    mqtt_port: int = field(default_factory=lambda: int(_env("MQTT_PORT", "1883")))
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092"))
    database_url: str = field(
        default_factory=lambda: _env(
            "DATABASE_URL", "postgresql://telemetry:telemetry@localhost:5432/telemetry"
        )
    )
    modbus_host: str = field(default_factory=lambda: _env("MODBUS_HOST", "localhost"))
    modbus_port: int = field(default_factory=lambda: int(_env("MODBUS_PORT", "5020")))
    site: str = field(default_factory=lambda: _env("SITE", "plant-a"))
    api_keys: frozenset[str] = field(
        default_factory=lambda: frozenset(
            k.strip() for k in _env("API_KEYS", "").split(",") if k.strip()
        )
    )


settings = Settings()
