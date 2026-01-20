from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = Field(
        default="postgresql+psycopg2://postgres:postgres@db:5432/postgres"
    )
    data_dir: str = Field(default="/app/data")
    uploads_dir: str = Field(default="/app/data/uploads")
    ocr_enabled: bool = Field(default=False)
    text_quality_threshold: int = Field(default=120)

    model_config = SettingsConfigDict(env_prefix="APP_", case_sensitive=False)


settings = Settings()
