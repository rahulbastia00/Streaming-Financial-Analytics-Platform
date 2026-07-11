from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

# 1. Finnhub
class FinnhubTick(BaseModel):
    s: str; p: float; t: int; v: float; c: List[str] = []

class FinnhubResponse(BaseModel):
    type: str; data: List[FinnhubTick]

# 2. Alpha Vantage
class AlphaVantageCandle(BaseModel):
    open: str = Field(..., alias="1. open")
    high: str = Field(..., alias="2. high")
    low: str = Field(..., alias="3. low")
    close: str = Field(..., alias="4. close")
    volume: str = Field(..., alias="5. volume")

class AlphaVantageResponse(BaseModel):
    meta_data: Dict[str, str] = Field(..., alias="Meta Data")
    time_series: Dict[str, AlphaVantageCandle] = Field(..., alias="Time Series (Daily)")

# 3. NewsAPI
class NewsSource(BaseModel):
    id: Optional[str]; name: str

class NewsArticle(BaseModel):
    source: NewsSource; author: Optional[str]; title: str; description: Optional[str]
    url: str; urlToImage: Optional[str]; publishedAt: str; content: Optional[str]

class NewsAPIResponse(BaseModel):
    status: str; totalResults: int; articles: List[NewsArticle]

# 4. MarketAux
class MarketAuxEntity(BaseModel):
    symbol: str; name: str; exchange: Optional[str]; sentiment_score: Optional[float]

class MarketAuxArticle(BaseModel):
    uuid: str; title: str; description: Optional[str]; snippet: Optional[str]; url: str
    image_url: Optional[str]; language: str; published_at: str; source: str; entities: List[MarketAuxEntity]

class MarketAuxResponse(BaseModel):
    meta: Dict[str, Any]; data: List[MarketAuxArticle]

# 5. AllTick
class AllTickDepthItem(BaseModel):
    price: str
    volume: str

class AllTickData(BaseModel):
    code: str
    seq: int
    tick_time: int
    bids: List[AllTickDepthItem] = []
    asks: List[AllTickDepthItem] = []

class AllTickResponse(BaseModel):
    cmd_id: int
    data: AllTickData