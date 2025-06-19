import requests
import pandas as pd
from pyspark.sql import SparkSession
from typing import Union
import os
from datetime import datetime
import random
import string
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from typing import Optional

class DataExtractor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def from_csv(self, file_path: str, **kwargs) -> DataFrame:
        """Carrega dados de um arquivo CSV"""
        return self.spark.read.option("header", "true").csv(file_path, **kwargs)
    
    def from_api(self, url: str, params: Optional[dict] = None) -> DataFrame:
        """Extrai dados de uma API REST"""
        response = requests.get(url, params=params)
        if response.status_code == 200:
            pandas_df = pd.DataFrame(response.json())
            return self.spark.createDataFrame(pandas_df)
        else:
            raise Exception(f"API request failed with status {response.status_code}")
    
    def from_database(self, jdbc_url: str, table: str, properties: dict) -> DataFrame:
        """Extrai dados de um banco de dados via JDBC"""
        return self.spark.read.jdbc(url=jdbc_url, table=table, properties=properties)
    
    def generate_simulated_data(self, num_records: int = 1000) -> DataFrame:
        """Gera dados simulados para testes"""
        data = []
        for _ in range(num_records):
            record = {
                "id": ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),
                "timestamp": datetime.now().isoformat(),
                "value": random.uniform(0, 100),
                "category": random.choice(["A", "B", "C", "D"]),
                "is_active": random.choice([True, False])
            }
            data.append(record)
        
        pandas_df = pd.DataFrame(data)
        return self.spark.createDataFrame(pandas_df)
    
    def from_kafka(self, bootstrap_servers: str, topic: str) -> DataFrame:
        """Lê dados de um tópico Kafka em streaming"""
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .load()