from datetime import datetime

from pydantic import BaseModel, Field, computed_field


class Quote(BaseModel):
    id: int
    timestamp: datetime
    name: str = Field(max_length=50)
    symbol: str = Field(max_length=6)
    date_added: datetime
    max_supply: int
    circulating_supply: float
    is_active: bool
    infinite_supply: bool
    minted_market_cap: float
    cmc_rank: int
    is_fiat: bool
    self_reported_circulating_supply: float
    self_reported_market_cap: float
    last_updated: datetime
    price: float
    volume_24h: float
    volume_change_24h: float
    percent_change_1h: float
    percent_change_24h: float
    percent_change_7d: float
    percent_change_30d: float
    percent_change_60d: float
    percent_change_90d: float
    market_cap: float
    market_cap_dominance: float
    fully_diluted_market_cap: float

    @computed_field
    @classmethod
    def ddl(cls) -> str:
        return "\n".join(
            [
                "CREATE TABLE quote (",

                ")",
            ]
        )


tables: list = [Quote]
