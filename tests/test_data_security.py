from pyspark.sql import SparkSession
from src.utils.data_security import DataSecurity
import pytest

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("TestSecurity") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_encrypt_column(spark):
    data = [("id1", "value1"), ("id2", "value2")]
    df = spark.createDataFrame(data, ["id", "value"])
    
    encrypted_df = DataSecurity.encrypt_column(df, "id")
    encrypted_ids = [row["id"] for row in encrypted_df.collect()]
    
    assert len(encrypted_ids[0]) == 64  # SHA-256 produces 64 char hash
    assert encrypted_ids[0] != "id1"

def test_mask_pii(spark):
    data = [("John Doe", "123 Main St"), ("Jane Smith", "456 Oak Ave")]
    df = spark.createDataFrame(data, ["name", "address"])
    
    masked_df = DataSecurity.mask_pii(df, ["name"])
    masked_names = [row["name"] for row in masked_df.collect()]
    
    assert masked_names[0] == "XXXXXXXX"
    assert masked_names[1] == "XXXXXXXXX"
    assert df.collect()[0]["address"] == "123 Main St"  # Address should remain unchanged