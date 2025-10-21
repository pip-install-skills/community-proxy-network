from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field

class Node(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    node_id: str
    public_key: Optional[str] = None
    ip: Optional[str] = None
    nat_type: Optional[str] = None
    country: Optional[str] = None
    last_seen: datetime = Field(default_factory=datetime.utcnow)
    credits: int = 0
    banned: bool = False

class MatchRequest(SQLModel):
    credits_required: int = 0
    country: Optional[str] = None