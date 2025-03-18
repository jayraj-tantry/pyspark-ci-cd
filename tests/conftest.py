import logging
import pytest
from solution.jay_solution import *

@pytest.fixture(scope="session")
def spark():
    spark = spark_session()
    yield spark
    spark.stop()
