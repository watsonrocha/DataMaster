from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.streaming import StreamingQuery
from typing import Dict, Union
import json
from .data_sources import DataExtractor

class DataIngestion:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.extractor = DataExtractor(spark)
        
    def batch_ingestion(self, source_config: Dict) -> DataFrame:
        """Processamento em lote (batch)"""
        source_type = source_config.get("type")
        
        if source_type == "csv":
            return self.extractor.from_csv(**source_config.get("params", {}))
        elif source_type == "api":
            return self.extractor.from_api(**source_config.get("params", {}))
        elif source_type == "database":
            return self.extractor.from_database(**source_config.get("params", {}))
        elif source_type == "simulated":
            return self.extractor.generate_simulated_data(**source_config.get("params", {}))
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def streaming_ingestion(self, source_config: Dict) -> Union[DataFrame, StreamingQuery]:
        """Processamento em tempo real (streaming)"""
        source_type = source_config.get("type")
        
        if source_type == "kafka":
            df = self.extractor.from_kafka(**source_config.get("params", {}))
            
            # Processamento básico para extrair valor da mensagem Kafka
            processed_df = df.selectExpr(
                "CAST(key AS STRING)",
                "CAST(value AS STRING)",
                "topic",
                "partition",
                "offset",
                "timestamp",
                "timestampType"
            )
            
            if source_config.get("output_mode") == "console":
                return processed_df.writeStream \
                    .outputMode("append") \
                    .format("console") \
                    .start()
            else:
                return processed_df
        else:
            raise ValueError(f"Unsupported streaming source type: {source_type}")
    
    def lambda_architecture_ingestion(self, batch_config: Dict, stream_config: Dict):
        """Implementação do padrão Lambda"""
        # Camada batch
        batch_df = self.batch_ingestion(batch_config)
        
        # Camada speed (streaming)
        stream_df = self.streaming_ingestion(stream_config)
        
        # União dos resultados (simplificado)
        # Na prática, você precisaria alinhar os esquemas e tempos
        return batch_df.union(stream_df)
    
    def kappa_architecture_ingestion(self, stream_config: Dict):
        """Implementação do padrão Kappa - tudo como stream"""
        return self.streaming_ingestion(stream_config)