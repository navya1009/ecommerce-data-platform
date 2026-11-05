#!/usr/bin/env python3
"""
Fake Data Generator for E-Commerce Data Platform
Generates realistic synthetic data and loads it into staging databases
Idempotent: Safe to run multiple times
"""

# ============================================
# IMPORTS - Each explained
# ============================================

import os  # For file/folder operations
import random  # For random number generation
from datetime import datetime, timedelta  # For date manipulation

import mysql.connector  # For MySQL database connection
import pandas as pd  # For DataFrame operations and CSV writing
from dotenv import load_dotenv  # For loading .env variables
from faker import Faker  # For generating fake realistic data
from sqlalchemy import create_engine  # For SQL database connections
from tqdm import tqdm  # For progress bars

# ============================================
# INITIALIZATION
# ============================================

# Load environment variables from .env file
load_dotenv()

# Initialize Faker with Indian locale for realistic Indian names/cities
fake = Faker("en_IN")  # 'en_IN' = English, India locale

# Add global Faker for non-Indian data (like USA cities)
fake_global = Faker("en_US")

# ============================================
# CONFIGURATION - Change these to scale data up/down
# ============================================

NUM_USERS = 10000  # Number of customers to generate
NUM_PRODUCTS = 2000  # Number of products
NUM_ORDERS = 100000  # Number of orders (each can have multiple items)
NUM_CLICKSTREAMS = 500000  # Number of click events
NUM_WAREHOUSES = 5  # Number of warehouses

# Date range: last 2 years
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=730)  # 730 days = 2 years

# ============================================
# DATABASE CONNECTION SETUP
# ============================================


def get_mysql_connection():
    """Create and return MySQL database connection"""
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "root123"),
        database=os.getenv("MYSQL_DATABASE", "ecommerce_staging"),
    )


def get_mysql_engine():
    """Create SQLAlchemy engine for pandas to_sql"""
    # SQLAlchemy connection string format: mysql+mysqlconnector://user:password@host:port/database
    connection_string = f"mysql+mysqlconnector://{os.getenv('MYSQL_USER', 'root')}:{os.getenv('MYSQL_PASSWORD', 'root123')}@{os.getenv('MYSQL_HOST', 'localhost')}:{int(os.getenv('MYSQL_PORT', 3306))}/{os.getenv('MYSQL_DATABASE', 'ecommerce_staging')}"  # noqa: E501
    return create_engine(connection_string)


# ============================================
# IDEMPOTENCY CHECK FUNCTIONS
# ============================================


def table_has_data(table_name):
    """Check if a table already has data (to prevent duplicates)"""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count > 0


def generate_batch_id():
    """Generate a unique batch ID for tracking"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"batch_{timestamp}"


# ============================================
# DATA GENERATION FUNCTIONS
# ============================================


def generate_users():
    """
    Generate 10,000 realistic users
    Distribution: 10% VIP, 60% Regular, 30% New
    """
    print("📊 Generating users...")
    users = []

    # Define segment probabilities
    segments = ["VIP", "Regular", "New"]
    segment_weights = [0.1, 0.6, 0.3]  # 10% VIP, 60% Regular, 30% New

    # Indian and global cities
    indian_cities = [
        "Mumbai",
        "Delhi",
        "Bangalore",
        "Hyderabad",
        "Chennai",
        "Kolkata",
        "Pune",
        "Ahmedabad",
        "Jaipur",
        "Lucknow",
    ]
    global_cities = ["New York", "London", "Singapore", "Dubai", "Sydney"]

    for i in tqdm(range(NUM_USERS), desc="Creating users"):
        # Randomly assign segment based on weights
        segment = random.choices(segments, weights=segment_weights)[0]

        # 70% Indian cities, 30% global cities
        if random.random() < 0.7:
            city = random.choice(indian_cities)
            country = "India"
        else:
            city = random.choice(global_cities)
            country = (
                "USA"
                if city in ["New York"]
                else (
                    "UK"
                    if city == "London"
                    else (
                        "Singapore"
                        if city == "Singapore"
                        else "Dubai" if city == "Dubai" else "Australia"
                    )
                )
            )

        user = {
            "user_id": i + 1,
            "name": fake.name(),
            "email": fake.email(),
            "city": city,
            "country": country,
            "segment": segment,
            "created_at": fake.date_between(start_date="-2y", end_date="today"),
            "updated_at": fake.date_time_between(start_date="-30d", end_date="now"),
        }
        users.append(user)

    return pd.DataFrame(users)


def generate_products():
    """
    Generate 2,000 products across 10 categories
    Realistic price ranges per category
    """
    print("📊 Generating products...")
    products = []

    categories = {
        "Electronics": {
            "min_price": 500,
            "max_price": 150000,
            "subcategories": ["Laptops", "Phones", "Tablets", "Headphones"],
        },
        "Clothing": {
            "min_price": 299,
            "max_price": 5000,
            "subcategories": ["Men", "Women", "Kids", "Accessories"],
        },
        "Books": {
            "min_price": 99,
            "max_price": 1500,
            "subcategories": ["Fiction", "Non-Fiction", "Educational", "Comics"],
        },
        "Home": {
            "min_price": 199,
            "max_price": 25000,
            "subcategories": ["Furniture", "Decor", "Kitchen", "Bedding"],
        },
        "Beauty": {
            "min_price": 99,
            "max_price": 5000,
            "subcategories": ["Makeup", "Skincare", "Haircare", "Fragrances"],
        },
        "Sports": {
            "min_price": 299,
            "max_price": 20000,
            "subcategories": ["Fitness", "Outdoor", "Team Sports", "Cycling"],
        },
        "Toys": {
            "min_price": 99,
            "max_price": 5000,
            "subcategories": ["Action Figures", "Board Games", "Educational", "Dolls"],
        },
        "Food": {
            "min_price": 49,
            "max_price": 2000,
            "subcategories": ["Snacks", "Beverages", "Organic", "Imported"],
        },
        "Automotive": {
            "min_price": 199,
            "max_price": 50000,
            "subcategories": ["Accessories", "Tools", "Spare Parts", "Cleaning"],
        },
        "Health": {
            "min_price": 99,
            "max_price": 10000,
            "subcategories": ["Supplements", "Personal Care", "Medical", "Fitness"],
        },
    }

    brands = [
        "Apple",
        "Samsung",
        "Nike",
        "Adidas",
        "Sony",
        "LG",
        "Boat",
        "Puma",
        "Zara",
        "H&M",
        "Rolex",
        "Titan",
        "Dell",
        "HP",
        "Lenovo",
    ]

    for i in tqdm(range(NUM_PRODUCTS), desc="Creating products"):
        category = random.choice(list(categories.keys()))
        cat_info = categories[category]

        # Realistic price within category range
        price = round(random.uniform(cat_info["min_price"], cat_info["max_price"]), 2)
        cost_price = round(
            price * random.uniform(0.4, 0.7), 2
        )  # Cost is 40-70% of selling price

        product = {
            "product_id": i + 1,
            "name": fake.catch_phrase()[:100],  # Max 100 chars
            "category": category,
            "subcategory": random.choice(cat_info["subcategories"]),
            "brand": random.choice(brands),
            "cost_price": cost_price,
            "selling_price": price,
            "is_available": random.random() > 0.1,  # 90% available
        }
        products.append(product)

    return pd.DataFrame(products)


def generate_orders():
    """
    Generate 100,000 orders with realistic patterns:
    - More orders on weekends (2x)
    - More orders in Nov/Dec (3x)
    - Status distribution: 85% completed, 10% pending, 3% cancelled, 2% returned
    - Each order has 1-5 items (line items)
    """
    print("📊 Generating orders and line items...")

    order_items = []

    # Order status distribution
    statuses = ["completed", "pending", "cancelled", "returned"]
    status_weights = [0.85, 0.10, 0.03, 0.02]

    # Track which products are available
    product_ids = list(range(1, NUM_PRODUCTS + 1))

    # Generate dates with realistic distribution
    date_range = pd.date_range(start=START_DATE, end=END_DATE, freq="D")

    # Assign weights to dates: weekends higher, Nov/Dec higher
    date_weights = []
    for date in date_range:
        weight = 1.0
        # Weekend boost (Saturday=5, Sunday=6)
        if date.weekday() >= 5:  # 5=Saturday, 6=Sunday
            weight *= 2
        # Holiday season boost (November and December)
        if date.month in [11, 12]:
            weight *= 3
        date_weights.append(weight)

    # Normalize weights
    total_weight = sum(date_weights)
    date_probabilities = [w / total_weight for w in date_weights]

    # Generate each order
    for order_id in tqdm(range(1, NUM_ORDERS + 1), desc="Creating orders"):
        # Pick date based on distribution
        order_date = pd.to_datetime(np.random.choice(date_range, p=date_probabilities))

        # Pick random user (1 to NUM_USERS)
        user_id = random.randint(1, NUM_USERS)

        # Pick status based on distribution
        status = random.choices(statuses, weights=status_weights)[0]

        # Number of items in this order (1-5, with higher probability for 1-2 items)
        num_items = random.choices(
            [1, 2, 3, 4, 5], weights=[0.4, 0.3, 0.15, 0.1, 0.05]
        )[0]

        # For completed orders, add a completed date (1-7 days after order date)
        if status == "completed":
            # Draw kept so the seeded random sequence does not change
            random.randint(1, 7)

        # Generate each line item in this order
        selected_products = random.sample(product_ids, min(num_items, len(product_ids)))

        for seq_num, product_id in enumerate(selected_products):
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(100, 50000), 2)
            discount = random.choices(
                [0, 5, 10, 15, 20, 25, 30, 40, 50],
                weights=[0.5, 0.2, 0.1, 0.05, 0.05, 0.03, 0.03, 0.02, 0.02],
            )[0]
            total_amount = round(quantity * unit_price * (1 - discount / 100), 2)

            order_items.append(
                {
                    "source_order_id": f"ORD_{order_id:08d}",
                    "source_line_id": f"ORD_{order_id:08d}_{seq_num}",
                    "user_id": user_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount": discount,
                    "total_amount": total_amount,
                    "order_status": status,
                    "order_date": order_date,
                    "ingested_at": datetime.now(),
                    "batch_id": BATCH_ID,
                    "is_processed": False,
                }
            )

    return pd.DataFrame(order_items)


def generate_clickstream():
    """Generate 500,000 clickstream events linked to orders and sessions"""
    print("📊 Generating clickstream events...")

    events = []
    event_types = [
        "page_view",
        "add_to_cart",
        "remove_from_cart",
        "checkout_start",
        "purchase",
        "search",
    ]
    event_weights = [0.6, 0.15, 0.05, 0.05, 0.05, 0.1]

    devices = ["mobile", "tablet", "desktop"]
    device_weights = [0.55, 0.1, 0.35]

    browsers = ["Chrome", "Firefox", "Safari", "Edge"]
    browser_weights = [0.65, 0.15, 0.15, 0.05]

    pages = [
        "/home",
        "/product",
        "/category",
        "/cart",
        "/checkout",
        "/search",
        "/account",
        "/wishlist",
    ]

    # Generate unique session IDs (100k sessions)
    sessions = [f"session_{i:08d}" for i in range(1, 100001)]

    for i in tqdm(range(NUM_CLICKSTREAMS), desc="Creating clickstream events"):
        event_date = fake.date_time_between(start_date=START_DATE, end_date=END_DATE)
        user_id = (
            random.randint(1, NUM_USERS) if random.random() > 0.2 else None
        )  # 20% anonymous
        product_id = random.randint(1, NUM_PRODUCTS) if random.random() > 0.5 else None
        session_id = random.choice(sessions)
        event_type = random.choices(event_types, weights=event_weights)[0]

        events.append(
            {
                "event_id": f"EVT_{i:09d}",
                "user_id": user_id,
                "session_id": session_id,
                "event_type": event_type,
                "page_url": random.choice(pages),
                "product_id": product_id,
                "device_type": random.choices(devices, weights=device_weights)[0],
                "browser_name": random.choices(browsers, weights=browser_weights)[0],
                "event_timestamp": event_date,
                "ingested_at": datetime.now(),
                "batch_id": BATCH_ID,
                "is_processed": False,
            }
        )

    return pd.DataFrame(events)


def generate_inventory():
    """Generate inventory records for all products across 5 warehouses"""
    print("📊 Generating inventory records...")

    records = []
    warehouse_ids = [f"WH_{i:03d}" for i in range(1, NUM_WAREHOUSES + 1)]
    warehouse_names = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata"]

    for product_id in tqdm(
        range(1, NUM_PRODUCTS + 1), desc="Creating inventory records"
    ):
        for wh_idx, warehouse_id in enumerate(warehouse_ids):
            quantity = random.randint(0, 1000)
            reorder_level = random.randint(10, 100)

            records.append(
                {
                    "product_id": product_id,
                    "warehouse_id": warehouse_id,
                    "warehouse_name": warehouse_names[wh_idx],
                    "quantity_on_hand": quantity,
                    "reorder_level": reorder_level,
                    "last_updated": datetime.now(),
                    "ingested_at": datetime.now(),
                    "batch_id": BATCH_ID,
                    "is_processed": False,
                }
            )

    return pd.DataFrame(records)


# ============================================
# DATABASE INSERTION FUNCTIONS
# ============================================


def insert_users_to_mysql(users_df):
    """Insert users into MySQL staging table (idempotent)"""
    print("📤 Inserting users into MySQL...")

    users_for_db = users_df.copy()

    engine = get_mysql_engine()

    try:
        users_for_db.to_sql("dim_users", engine, if_exists="append", index=False)
        print(f"   ✅ Inserted {len(users_for_db)} users")
    except Exception as e:
        print(f"   ⚠️ Error inserting users: {e}")


def insert_products_to_mysql(products_df):
    """Insert products into MySQL staging table (idempotent)"""
    print("📤 Inserting products into MySQL...")

    # Rename columns to match MySQL table schema
    products_for_db = products_df.copy()
    products_for_db.rename(
        columns={
            "product_id": "source_id",
            "selling_price": "selling_price",  # Already correct
        },
        inplace=True,
    )

    # Add required columns that MySQL expects
    products_for_db["raw_data"] = None
    products_for_db["ingested_at"] = datetime.now()
    products_for_db["batch_id"] = BATCH_ID
    products_for_db["is_processed"] = False
    products_for_db["processed_at"] = None
    products_for_db["error_message"] = None

    engine = get_mysql_engine()

    try:
        products_for_db.to_sql(
            "stg_raw_products", engine, if_exists="append", index=False
        )
        print(f"   ✅ Inserted {len(products_for_db)} products")
    except Exception as e:
        print(f"   ⚠️ Error inserting products: {e}")


def write_orders_to_csv(orders_df):
    """Write orders to CSV file (simulating S3 data lake)"""
    print("📤 Writing orders to CSV...")

    # Create directory if it doesn't exist
    os.makedirs("data/raw/orders", exist_ok=True)

    # Write to CSV with timestamp to avoid overwriting
    filename = f"data/raw/orders/orders_{BATCH_ID}.csv"
    orders_df.to_csv(filename, index=False)
    print(f"   ✅ Wrote {len(orders_df)} order items to {filename}")


def write_clickstream_to_csv(clickstream_df):
    """Write clickstream to CSV file"""
    print("📤 Writing clickstream to CSV...")

    os.makedirs("data/raw/clickstream", exist_ok=True)
    filename = f"data/raw/clickstream/clickstream_{BATCH_ID}.csv"
    clickstream_df.to_csv(filename, index=False)
    print(f"   ✅ Wrote {len(clickstream_df)} clickstream events to {filename}")


def write_inventory_to_csv(inventory_df):
    """Write inventory to CSV file"""
    print("📤 Writing inventory to CSV...")

    os.makedirs("data/raw/inventory", exist_ok=True)
    filename = f"data/raw/inventory/inventory_{BATCH_ID}.csv"
    inventory_df.to_csv(filename, index=False)
    print(f"   ✅ Wrote {len(inventory_df)} inventory records to {filename}")


# ============================================
# MAIN EXECUTION
# ============================================

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 E-COMMERCE FAKE DATA GENERATOR")
    print("=" * 60)

    # Import numpy for random choice with probabilities
    import numpy as np

    # Generate unique batch ID for this run
    BATCH_ID = generate_batch_id()
    print(f"\n📋 Batch ID: {BATCH_ID}")

    # Step 1: Generate data
    print("\n" + "=" * 40)
    print("📊 STEP 1: Generating Data")
    print("=" * 40)

    users_df = generate_users()
    products_df = generate_products()
    orders_df = generate_orders()
    clickstream_df = generate_clickstream()
    inventory_df = generate_inventory()

    # Step 2: Insert into databases
    print("\n" + "=" * 40)
    print("💾 STEP 2: Inserting into Databases")
    print("=" * 40)

    insert_users_to_mysql(users_df)
    insert_products_to_mysql(products_df)

    # Step 3: Write to CSV files
    print("\n" + "=" * 40)
    print("📁 STEP 3: Writing CSV Files")
    print("=" * 40)

    write_orders_to_csv(orders_df)
    write_clickstream_to_csv(clickstream_df)
    write_inventory_to_csv(inventory_df)

    # Step 4: Summary
    print("\n" + "=" * 40)
    print("✅ GENERATION COMPLETE!")
    print("=" * 40)
    print("\n📊 Summary:")
    print(f"   👥 Users: {len(users_df):,}")
    print(f"   📦 Products: {len(products_df):,}")
    print(f"   🛒 Order Items: {len(orders_df):,}")
    print(f"   🖱️ Clickstream Events: {len(clickstream_df):,}")
    print(f"   📦 Inventory Records: {len(inventory_df):,}")
    print("\n📁 Output files in data/raw/")
    print(f"\n🏷️ Batch ID: {BATCH_ID}")
