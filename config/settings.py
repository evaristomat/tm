import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    BASE_URL = os.getenv("BASE_URL", "https://api.betsapi.com/v1")
    BETSAPI_API_KEY = os.getenv("BETSAPI_API_KEY")
    REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))


settings = Settings()
