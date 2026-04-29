from pydantic import BaseModel
from typing import List


class RevenueByUser(BaseModel):
    user_id: int
    total_orders: int
    total_quantity: int
    total_revenue: float


class ReportResponse(BaseModel):
    page: int
    limit: int
    total_rows: int
    total_revenue: float
    synced_orders: int
    pending_orders: int
    revenue_by_user: List[RevenueByUser]
    orders: list