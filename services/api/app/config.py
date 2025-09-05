from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    API_FOOTBALL_KEY: str = Field(..., description="api-football 密钥")
    DATABASE_URL: str = Field(..., description="Postgres 连接串")
    REDIS_URL: str = Field(..., description="Redis 连接串")
    TELEGRAM_BOT_TOKEN: str = Field(..., description="Telegram Bot Token")
    TELEGRAM_CHAT_ID: str = Field(..., description="Telegram Chat ID")
    HMAC_SECRET: str = Field(..., description="HMAC 签名密钥")

    TIMEZONE: str = Field("Asia/Taipei", description="默认时区")
    ENV: str = Field("prod", description="环境标识：dev/prod")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
