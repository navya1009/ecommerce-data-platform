"""
Pydantic models for API request/response schemas
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field

# ============================================
# ORDER MODELS
# ============================================


class OrderItemSchema(BaseModel):
    """Schema for individual order item in API response"""

    order_id: str = Field(..., description="Order ID from source system")
    line_id: str = Field(..., description="Line item ID")
    user_id: int = Field(..., description="Customer ID")
    product_id: int = Field(..., description="Product ID")
    quantity: int = Field(..., ge=1, le=100, description="Quantity ordered")
    unit_price: float = Field(..., gt=0, description="Price per unit")
    discount: float = Field(0, ge=0, le=100, description="Discount percentage")
    order_status: str = Field(
        ..., description="Order status: pending, completed, cancelled, returned"
    )
    order_date: str = Field(..., description="Order date in ISO format")


class OrderResponseSchema(BaseModel):
    """Schema for paginated order API response"""

    data: List[OrderItemSchema] = Field(..., description="List of order items")
    total: int = Field(..., description="Total number of records matching query")
    page: int = Field(..., description="Current page number")
    limit: int = Field(..., description="Number of records per page")
    total_pages: int = Field(..., description="Total number of pages")
    next_cursor: Optional[str] = Field(
        None, description="Cursor for next page (last order_date_iso)"
    )


class OrderRequestSchema(BaseModel):
    """Schema for order API request parameters"""

    since: Optional[datetime] = Field(
        None, description="Extract orders since this timestamp"
    )
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(100, ge=1, le=500, description="Records per page")


# ============================================
# USER MODELS
# ============================================


class UserSchema(BaseModel):
    """Schema for user data in API response"""

    user_id: int
    name: str
    email: str
    city: Optional[str]
    country: Optional[str]
    segment: str
    created_at: str
    updated_at: Optional[str]


class UserResponseSchema(BaseModel):
    """Schema for user API response"""

    data: List[UserSchema]
    updated_since: Optional[str]
    has_more: bool
    next_cursor: Optional[str]


class UserRequestSchema(BaseModel):
    """Schema for user API request parameters"""

    updated_since: Optional[datetime] = Field(
        None, description="Get users updated since this timestamp"
    )


# ============================================
# HEALTH MODEL
# ============================================


class HealthResponseSchema(BaseModel):
    """Schema for health check response"""

    status: str = Field("ok", description="Service status")
    timestamp: str = Field(..., description="Current timestamp in ISO format")
    version: str = Field("1.0.0", description="API version")
    database: str = Field(..., description="Database connection status")
