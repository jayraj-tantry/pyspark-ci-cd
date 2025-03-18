import logging
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, col, count

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def spark_session():
    """
    Create and return a SparkSession.
    """
    spark = SparkSession.builder.master("local[*]").appName("jay_solution").getOrCreate()
    logging.info("SparkSession created.")
    return spark



def load_customers(spark: SparkSession, data_path: str) -> DataFrame:
    """Function to read the Customers data"""
    try:
        df = spark.read.format("csv").option("header", True).load(data_path)
        logging.info("Customers data loaded successfully from %s", data_path)
        return df
    except Exception as e:
        logging.error("Error loading customers data - %s", str(e))
        raise

def load_products(spark: SparkSession, data_path: str) -> DataFrame:
    """Function to read the Products data"""
    try:
        df = spark.read.format("csv").option("header", True).load(data_path)
        logging.info("Products data loaded successfully from %s", data_path)
        return df
    except Exception as e:
        logging.error("Error loading products data - %s", str(e))
        raise

def load_transactions(spark: SparkSession, data_path: str) -> DataFrame:
    """Function to read the Transactions data"""
    try:
        df = spark.read.format("json").option("multiLine", True).load(data_path)
        logging.info("Transactions data loaded successfully from %s", data_path)
        return df
    except Exception as e:
        logging.error("Error loading transactions data - %s", str(e))
        raise

def flatten_transactions(transactions_df: DataFrame) -> DataFrame:
    """Function to flatten the transactions data that contains basket of products"""
    try:
        exploded_df = transactions_df.withColumn("basket_items", explode(col("basket")))
        flat_df = exploded_df.select(
            "customer_id",
            col("basket_items.product_id").alias("product_id"),
            col("basket_items.price").alias("price"),
            "date_of_purchase"
        )
        logging.info("transaction data flattened successfully.")
        return flat_df
    except Exception as e:
        logging.error("Error flattening transactions data - %s", str(e))


def join_data(customers_df: DataFrame, products_df: DataFrame, transactions_df: DataFrame) -> DataFrame:
    """Function to join the given three Datasets"""
    try:
        joined_df = transactions_df.join(customers_df, transactions_df.customer_id == customers_df.customer_id, "inner")\
                                .join(products_df, transactions_df.product_id == products_df.product_id, "inner")\
                                .drop(customers_df.customer_id, products_df.product_id)
        logging.info("Data joined successfully.")
        return joined_df
    except Exception as e:
        logging.error("Error joining the data - %s", str(e))
        raise

def aggregated_data(joined_df: DataFrame) -> DataFrame:
    """Function to calculate the frequency of purchases for a given customer"""
    try:
        agg_df = joined_df.groupBy("customer_id", "loyalty_score", "product_id", "product_category")\
                        .agg(count("date_of_purchase").alias("purchase_count"))
        logging.info("Data aggregated successfully.")
        return agg_df
    except Exception as e:
        logging.error("Error aggregating the data - %s", str(e))
        raise

def load_output_data(agg_df: DataFrame, output_path: str) -> None:
    """Function to write the aggregated data to the output location"""
    try:
        agg_df.write.format("csv").mode("overwrite").option("header", True).save(output_path)
        logging.info("Data written to output location %s", output_path)
    except Exception as e:
        logging.error("Error writing data to output location - %s", str(e))
        raise

def run_pipeline():
    spark = spark_session()

    customers_data_path = r"./input_data/starter/customer.csv"
    products_data_path = r"./input_data/starter/products.csv"
    transactions_data_path = r"./input_data/starter/transactions/*/transactions.json"
    output_path = r"./output_data/user_story_1"

    try:
        customers_df = load_customers(spark, customers_data_path)
        products_df = load_products(spark, products_data_path)

        
        raw_transactions_df = load_transactions(spark, transactions_data_path)
        final_transactions_df = flatten_transactions(raw_transactions_df)

        joined_df = join_data(customers_df, products_df, final_transactions_df)
        agg_df = aggregated_data(joined_df)

        load_output_data(agg_df, output_path)
    except Exception as e:
        logging.error("Error running the pipeline - %s", str(e))
        sys.exit(1)
    finally:
        spark.stop()
        logging.info("SparkSession stopped.")


if __name__ == "__main__":
    run_pipeline()
    


