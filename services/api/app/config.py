from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, AliasChoices

class Settings(BaseSettings):
    # 允许两种写法：API_FOOTBALL_KEY 或 APIFOOTBALL_KEY
    API_FOOTBALL_KEY: str | None = Field(
        default=None,
        validation_alias=AliasChoices("API_FOOTBALL_KEY", "APIFOOTBALL_KEY"),
        description="api-football 密钥"
    )

    # 其余全部改为可空，应用先启动；/healthz 再提示缺哪项
    DATABASE_URL: str | None = Field(default=None, description="Postgres 连接串")
    REDIS_URL: str | None = Field(default=None, description="Redis 连接串")
    TELEGRAM_BOT_TOKEN: str | None = Field(default=None, description="Telegram Bot Token")
    TELEGRAM_CHAT_ID: str | None = Field(default=None, description="Telegram Chat ID")
    HMAC_SECRET: str | None = Field(default=None, description="HMAC 签名密钥")

    TIMEZONE: str = Field("Asia/Taipei", description="默认时区")
    ENV: str = Field("prod", description="环境标识：dev/prod")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

settings = Settings()
