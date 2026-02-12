import json
import logging
import os
import shutil
from functools import cache
from pathlib import Path

from pydantic import BaseModel, computed_field, field_validator

logger = logging.getLogger(__name__)
CONFIG_PATH = Path(os.getenv("HOME")).joinpath(".config/cryptomonere/config.json")


class Config(BaseModel):
    config_dir: Path = CONFIG_PATH.parent
    data_dir: Path = CONFIG_PATH.parent
    symbols: list[str] = ["BTC", "ETH", "BCH", "XMR"]
    api_keys: dict

    # Handy checks before initializing Config, part of Pydantic lib
    @field_validator("data_dir", mode="before")
    @classmethod
    def validate_data_dir_is_dir_not_file(cls, value: str) -> Path:
        p = Path(value).expanduser()
        p.mkdir(parents=True, exist_ok=True)
        if not p.is_dir():
            raise Exception("data_dir must be a directory, not a file")
        return p

    @computed_field
    @property
    def sqlite_db_path(self) -> Path:
        return self.data_dir.joinpath("cryptocurrency_tracker.db")


@cache  # cache will cache the results in an in-memory python dictionary. Literally no faster data structure possible.
def get_config():
    if not CONFIG_PATH.exists():
        build_config()

    # Load hard-coded config path
    with open(CONFIG_PATH) as f:
        return Config.model_validate(json.load(f))


def build_config():
    CONFIG_PATH.parent.mkdir(exist_ok=True, parents=True)
    logger.info("Config file does not exist. Creating default config file.")
    shutil.copy(Path(__file__).parent.joinpath("config_default.json"), CONFIG_PATH)
    shutil.copy(Path(__file__).parent.joinpath("alerts_sample.json"), CONFIG_PATH.parent.joinpath("alert_rules.json"))
    logger.info(
        f"""Config file has been created at {CONFIG_PATH}.\n
    Please open your config file and update your api key.
    Please Also update your alert_rules.json file to get correct alerts"""
    )
    exit()


def save_config(config: Config) -> None:
    with open(CONFIG_PATH, "w") as f:
        # Dump Pydantic model as JSON to hard-coded config path
        f.write(config.model_dump_json())
        # Clear cache after writing new config
        get_config.cache_clear()


# Don't put an "if __name__ == '__main__'" block in every file. It literally does nothing and would never be used/useable.
# You only need it in main.py, because that is __main__, by definition.
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    c = get_config()
    print(c)
