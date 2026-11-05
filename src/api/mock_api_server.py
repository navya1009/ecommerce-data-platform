# isort: skip_file
"""
Mock FastAPI Server for E-Commerce Order API
Simulates real API behavior for local development
"""

import asyncio
import os
import random
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import mysql.connector
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from mysql.connector import Error as MySQLError

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from models.api_models import OrderResponseSchema  # noqa: E402
from models.api_models import HealthResponseSchema, UserResponseSchema  # noqa: E402

load_dotenv()

# ============================================
# DATABASE HELPERS
# ============================================


def _required_env(name: str) -> str:
    """Credentials have no built-in default. Fail with a clear message if unset."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"{name} must be set in the environment (no default is built in)"
        )
    return value


def get_mysql_connection():
    """Create MySQL database connection"""
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER", "root"),
        password=_required_env("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE", "ecommerce_staging"),
    )


def get_db_cursor(dictionary=True):
    """Get MySQL cursor with dict output"""
    conn = get_mysql_connection()
    return conn, conn.cursor(dictionary=dictionary) if dictionary else conn.cursor()


# ============================================
# API ERROR HANDLING
# ============================================


class APIException(Exception):
    """Custom API exception"""

    def __init__(self, status_code: int, detail: str, error_type: str = "api_error"):
        self.status_code = status_code
        self.detail = detail
        self.error_type = error_type


# ============================================
# FASTAPI APPLICATION
# ============================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown"""
    print("🚀 Starting Mock API Server...")
    print(
        f"📊 Using MySQL: {os.getenv('MYSQL_HOST', 'localhost')}:{os.getenv('MYSQL_PORT', 3306)}"
    )
    print(f"📁 Database: {os.getenv('MYSQL_DATABASE', 'ecommerce_staging')}")
    yield
    print("👋 Shutting down Mock API Server...")


app = FastAPI(
    title="E-Commerce Mock API",
    description="Mock API for local development and testing of data pipelines",
    version="1.0.0",
    lifespan=lifespan,
)

# ============================================
# MIDDLEWARE - Simulate latency and errors
# ============================================


@app.middleware("http")
async def add_latency_and_errors(request: Request, call_next):
    """
    Middleware to:
    1. Add random latency (50-200ms)
    2. Randomly return 429 (5%) or 503 (5%)
    """
    # Simulate latency
    latency_ms = random.randint(50, 200)
    await asyncio.sleep(latency_ms / 1000)

    # Randomly return errors (5% each)
    rand_val = random.random()
    if rand_val < 0.05:  # 5% chance - 429 Too Many Requests
        return JSONResponse(
            status_code=429,
            content={
                "error": "too_many_requests",
                "message": "Rate limit exceeded. Please try again later.",
                "retry_after": 60,
            },
        )
    elif rand_val < 0.10:  # 5% chance - 503 Service Unavailable
        return JSONResponse(
            status_code=503,
            content={
                "error": "service_unavailable",
                "message": "The service is temporarily unavailable. Please try again.",
            },
        )

    # Normal response
    response = await call_next(request)
    return response


# ============================================
# HEALTH CHECK ENDPOINT
# ============================================


@app.get("/health", response_model=HealthResponseSchema)
async def health_check():
    """Health check endpoint"""
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"

    return {
        "status": "ok" if db_status == "connected" else "degraded",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0",
        "database": db_status,
    }


# ============================================
# ORDERS ENDPOINT
# ============================================


@app.get("/orders", response_model=OrderResponseSchema)
async def get_orders(
    since: Optional[str] = Query(
        None, description="Extract orders since this timestamp (ISO format)"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=500, description="Records per page"),
):
    """
    Get paginated orders with watermark-based incremental extraction.
    Supports pagination with next_cursor for cursor-based navigation.
    """
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(dictionary=True)

        # Build WHERE clause
        where_clauses = []
        params = []

        if since:
            where_clauses.append("order_date > %s")
            params.append(since)

        # Only get unprocessed orders (or all if no watermark)
        # For mock API, we'll get all orders with optional since filter

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # Get total count
        count_query = f"""
            SELECT COUNT(*) as total
            FROM stg_raw_orders
            {where_sql}
        """
        cursor.execute(count_query, params)
        total = cursor.fetchone()["total"]

        # Calculate pagination
        offset = (page - 1) * page_size
        total_pages = (total + page_size - 1) // page_size

        # Get data
        query = f"""
            SELECT
                source_order_id as order_id,
                source_line_id as line_id,
                user_id,
                product_id,
                quantity,
                unit_price,
                discount,
                order_status,
                DATE_FORMAT(order_date, '%Y-%m-%dT%TZ') as order_date
            FROM stg_raw_orders
            {where_sql}
            ORDER BY order_date DESC
            LIMIT %s OFFSET %s
        """
        params.extend([page_size, offset])
        cursor.execute(query, params)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        # Create next_cursor from last record's order_date
        next_cursor = None
        if rows and page < total_pages:
            # Use the last record's order_date as cursor
            next_cursor = rows[-1].get("order_date")

        return {
            "data": rows,
            "total": total,
            "page": page,
            "limit": page_size,
            "total_pages": total_pages,
            "next_cursor": next_cursor,
        }

    except MySQLError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# ============================================
# USERS ENDPOINT
# ============================================


@app.get("/users", response_model=UserResponseSchema)
async def get_users(
    updated_since: Optional[str] = Query(
        None, description="Get users updated since this timestamp (ISO format)"
    )
):
    """
    Get users with optional updated_since filter for CDC extraction.
    """
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(dictionary=True)

        # Build WHERE clause
        where_clauses = []
        params = []

        if updated_since:
            where_clauses.append("updated_at > %s")
            params.append(updated_since)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        query = f"""
            SELECT
                user_id,
                name,
                email,
                city,
                country,
                segment,
                DATE_FORMAT(created_at, '%Y-%m-%dT%TZ') as created_at,
                DATE_FORMAT(updated_at, '%Y-%m-%dT%TZ') as updated_at
            FROM dim_users
            {where_sql}
            ORDER BY user_id
        """
        cursor.execute(query, params)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        return {
            "data": rows,
            "updated_since": updated_since,
            "has_more": False,
            "next_cursor": None,
        }

    except MySQLError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# ============================================
# ERROR HANDLERS
# ============================================


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": "http_error",
            "status_code": exc.status_code,
            "detail": exc.detail,
        },
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500, content={"error": "internal_server_error", "detail": str(exc)}
    )


# ============================================
# RUN SERVER
# ============================================

if __name__ == "__main__":
    uvicorn.run("mock_api_server:app", host="0.0.0.0", port=8000, reload=True)
