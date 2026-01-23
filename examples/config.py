import json
import os
from functools import cache
from pathlib import Path

from pydantic import BaseModel, computed_field, field_validator

CONFIG_PATH = Path("{home}/.cryptocurrency_tracker/config.json".format(home=os.getenv("HOME")))


class Config(BaseModel):
    data_dir: Path = CONFIG_PATH.parent
    symbols: list[str] = ["BTC", "ETH", "BCH", "XMR", "SOL", "MINA", "ZEC", "BNB", "XRP", "AGIX", "CXTC", "PAXG", "XAUT", "KAG"]

    # Handy checks before initializing Config, part of Pydantic lib
    @field_validator("data_dir", mode="before")
    @classmethod
    def validate_data_dir_is_dir_not_file(cls, value: str) -> Path:
        p = Path(value)
        if p.exists() and not p.is_dir():
            raise Exception("data_dir must be a directory, not a file")
        return p

    @computed_field
    @property
    def sqlite_db_path(self) -> Path:
        return self.data_dir.joinpath("cryptocurrency_tracker.db")


@cache  # cache will cache the results in an in-memory python dictionary. Literally no faster data structure possible.
def get_config():

    # Create parent directory if not exists
    if not CONFIG_PATH.exists():
        CONFIG_PATH.parent.mkdir(exist_ok=True)

    # Load hard-coded config path
    with open(CONFIG_PATH) as f:
        return Config.model_validate(json.load(f))


def save_config(config: Config) -> None:
    with open(CONFIG_PATH, "w") as f:
        # Dump Pydantic model as JSON to hard-coded config path
        f.write(config.model_dump_json())
        # Clear cache after writing new config
        get_config.cache_clear()


# Don't put an "if __name__ == '__main__'" block in every file. It literally does nothing and would never be used/useable.
# You only need it in main.py, because that is __main__, by definition.
