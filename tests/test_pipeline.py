import pytest
from solution.jay_solution import *


@pytest.mark.usefixtures("spark")
def test_load_customers(spark):
    print("Running test_load_customers to check if customers data is loaded.")
    customers_df = load_customers(spark, r"./tests/data/customers.csv")
    assert customers_df.count() == 2

@pytest.mark.usefixtures("spark")
def test_load_products(spark):
    print("Running test_load_products to check if products data is loaded.")
    products_df = load_products(spark, r"./tests/data/products.csv")
    assert products_df.count() == 2

@pytest.mark.usefixtures("spark")
def test_load_transactions(spark):
    print("Running test_load_transactions to check if transactions data is loaded.")
    transactions_df = load_transactions(spark, r"./tests/data/transactions.json")
    assert transactions_df.count() == 1


@pytest.mark.usefixtures("spark")
def test_flatten_transactions(spark):
    print("Running test_flatten_transactions to check if basket column is flattened.")
    data = [
        {"customer_id": "C1", "basket": [{"product_id": "P1", "price": 1000}], "date_of_purchase": "2025-03-17 08:44:12.727771"},
        {"customer_id": "C2", "basket": [{"product_id": "P2", "price": 1100},{"product_id": "P3", "price": 1200}], "date_of_purchase": "2024-12-16 08:44:12.727771"}
    ]

    transactions_df = spark.createDataFrame(data)

    transformed_df = flatten_transactions(transactions_df)

    result = transformed_df.collect()

    product_ids = [row.product_id for row in result]
    assert product_ids == ["P1", "P2", "P3"]


@pytest.mark.usefixtures("spark")
def test_agg_data(spark):
    print("Running test_agg_data to check if purchase_count is calculated correctly")
    data = [
        {"customer_id": "C1", "loyalty_score": 10, "product_id": "P1", "product_category": "Electronics", "date_of_purchase":"2025-03-17"},
        {"customer_id": "C1", "loyalty_score": 10, "product_id": "P1", "product_category": "Electronics", "date_of_purchase":"2025-03-18"},
        {"customer_id": "C1", "loyalty_score": 10, "product_id": "P4", "product_category": "Electronics", "date_of_purchase":"2025-03-19"},
        {"customer_id": "C2", "loyalty_score": 30, "product_id": "P2", "product_category": "Clothing", "date_of_purchase":"2025-03-18"},
        {"customer_id": "C3", "loyalty_score": 40, "product_id": "P3", "product_category": "Grocery", "date_of_purchase":"2025-03-19"}
    ]

    joined_df = spark.createDataFrame(data)
    agg_data = aggregated_data(joined_df)

    result = agg_data.collect()

    purchase_counts = [row.purchase_count for row in result]
    assert 1 in purchase_counts
    assert purchase_counts == [2,1,1,1]