from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Optional
from delta.tables import DeltaTable
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql import DataFrame


class DataStorage:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def store_parquet(self, df: DataFrame, path: str, mode: str = "overwrite", partition_by: Optional[list] = None):
        """Armazena dados em formato Parquet"""
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(path)
    
    def store_delta(self, df: DataFrame, path: str, mode: str = "overwrite", partition_by: Optional[list] = None):
        """Armazena dados em formato Delta Lake"""
        writer = df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)

    def stream_to_delta(self,
        df: DataFrame,
        path: str,
        checkpoint_location: str,
        output_mode: str = "append"
    ) -> StreamingQuery:  return df.writeStream \
        .format("delta") \
        .outputMode(output_mode) \
        .option("checkpointLocation", checkpoint_location) \
        .start(path)
    
    def store_jdbc(self, df: DataFrame, jdbc_url: str, table: str, properties: Dict, mode: str = "overwrite"):
        """Armazena dados em um banco de dados via JDBC"""
        df.write.jdbc(url=jdbc_url, table=table, mode=mode, properties=properties)
    
    def create_delta_table(self, path: str, table_name: str, database: str = "default"):
        """Cria uma tabela Delta gerenciada"""
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
        df = self.spark.read.format("delta").load(path)
        df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(f"{database}.{table_name}")
    
    def upsert_to_delta(self, new_data: DataFrame, path: str, merge_key: str):
        """Realiza upsert em uma tabela Delta"""
        delta_table = DeltaTable.forPath(self.spark, path)
        
        delta_table.alias("target").merge(
            new_data.alias("source"),
            f"target.{merge_key} = source.{merge_key}"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()