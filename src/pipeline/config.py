"""Runtime configuration.

Everything comes from the environment so the same image runs locally, in CI and
in Airflow without code changes. Secrets are never defaulted to a real value.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- source API -------------------------------------------------------
    weather_api_base_url: str = "https://archive-api.open-meteo.com/v1"
    weather_api_timeout_seconds: float = 30.0
    weather_api_max_retries: int = 5
    # The producer publishes history with a ~2 day lag; asking for anything
    # newer returns an empty series that would look like data loss downstream.
    source_lag_days: int = 2

    # --- landing zone -----------------------------------------------------
    landing_zone_root: Path = Path("./data/landing")

    # --- Snowflake --------------------------------------------------------
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: SecretStr = SecretStr("")
    snowflake_role: str = "TRANSFORMER"
    snowflake_warehouse: str = "WH_INGEST_XS"
    snowflake_database: str = "WEATHER"
    snowflake_raw_schema: str = "RAW"

    # --- behaviour --------------------------------------------------------
    pipeline_env: str = Field(default="dev", pattern="^(dev|ci|prod)$")
    log_level: str = "INFO"
    contracts_dir: Path = Path("contracts")

    @property
    def raw_fqn(self) -> str:
        """Fully qualified name of the RAW schema."""
        return f"{self.snowflake_database}.{self.snowflake_raw_schema}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached accessor so config is parsed once per process."""
    return Settings()
