#!/usr/bin/env python3
"""
Debug script to find which expectation is failing in raw_orders_suite
"""
import json

import pandas as pd
from sqlalchemy import create_engine

import great_expectations as gx

# Load data
engine = create_engine(
    "mysql+mysqlconnector://root:root123@mysql:3306/ecommerce_staging"
)
df = pd.read_sql("SELECT * FROM stg_raw_orders LIMIT 1000", engine)

print(f"Loaded {len(df)} rows")
print(f"Columns: {list(df.columns)}")
print()

# Create GE dataset
ge_df = gx.from_pandas(df)

# Load suite
with open("/app/great_expectations/expectations/raw_orders_suite.json", "r") as f:
    suite_config = json.load(f)

# Apply each expectation manually and report
for exp in suite_config["expectations"]:
    exp_type = exp["expectation_type"]
    kwargs = exp["kwargs"]

    print(f"Testing: {exp_type} with {kwargs}")

    try:
        if exp_type == "expect_column_to_exist":
            result = ge_df.expect_column_to_exist(**kwargs)
        elif exp_type == "expect_column_values_to_not_be_null":
            result = ge_df.expect_column_values_to_not_be_null(**kwargs)
        elif exp_type == "expect_column_values_to_be_unique":
            result = ge_df.expect_column_values_to_be_unique(**kwargs)
        elif exp_type == "expect_column_values_to_be_between":
            result = ge_df.expect_column_values_to_be_between(**kwargs)
        elif exp_type == "expect_column_values_to_be_in_set":
            result = ge_df.expect_column_values_to_be_in_set(**kwargs)
        elif exp_type == "expect_table_row_count_to_be_between":
            result = ge_df.expect_table_row_count_to_be_between(**kwargs)

        status = "✅ PASSED" if result.success else "❌ FAILED"
        print(f"  {status}")

        if not result.success:
            print(f"  Details: {result.result}")
            print(f"  Unexpected count: {result.result.get('unexpected_count', 'N/A')}")
            print(
                f"  Unexpected percent: {result.result.get('unexpected_percent', 'N/A')}%"
            )
    except Exception as e:
        print(f"  ❌ ERROR: {e}")

    print()
