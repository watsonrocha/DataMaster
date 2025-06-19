from pyspark.sql import SparkSession
from src.ingestion.pipeline import DataIngestion
from src.storage.data_lake import DataStorage
from src.monitoring.observability import DataObservability
from src.utils.data_security import DataSecurity
from config.settings import Settings
import mlflow
import logging

# Configuração básica de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    """Cria e configura a sessão do Spark"""
    return SparkSession.builder \
        .master(Settings.SPARK_MASTER) \
        .appName(Settings.SPARK_APP_NAME) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def run_pipeline():
    """Executa o pipeline completo de dados"""
    spark = create_spark_session()
    observability = DataObservability(spark)
    storage = DataStorage(spark)
    
    # Inicia o tracking do MLflow
    mlflow.set_tracking_uri("http://localhost:5000")
    mlflow.set_experiment("Data Pipeline Monitoring")
    
    with mlflow.start_run():
        try:
            # 1. Ingestão de dados
            ingestion = DataIngestion(spark)
            
            # Configuração para dados batch
            batch_config = {
                "type": "simulated",
                "params": {"num_records": 10000}
            }
            
            # Configuração para dados streaming
            stream_config = {
                "type": "kafka",
                "params": {
                    "bootstrap_servers": Settings.KAFKA_BOOTSTRAP_SERVERS,
                    "topic": Settings.KAFKA_TOPIC
                },
                "output_mode": "delta"
            }
            
            # Executa ingestão batch
            batch_df = observability.monitor_performance(
                ingestion.batch_ingestion, batch_config
            )
            observability.track_data_lineage(batch_df, "batch_ingestion")
            
            # Aplica segurança de dados
            secure_df = DataSecurity.encrypt_column(batch_df, "id", Settings.ENCRYPTION_SALT)
            secure_df = DataSecurity.mask_pii(secure_df, ["category"])
            
            # Armazena dados batch
            batch_path = f"{Settings.DATA_LAKE_PATH}/batch_data"
            observability.monitor_performance(
                storage.store_delta, secure_df, batch_path
            )
            
            # Executa ingestão streaming (simulado)
            # Em produção, isso seria executado continuamente
            stream_df = ingestion.streaming_ingestion(stream_config)
            stream_path = f"{Settings.DATA_LAKE_PATH}/stream_data"
            checkpoint_path = f"{Settings.CHECKPOINT_PATH}/stream_checkpoint"
            
            # Inicia o streaming (em produção seria awaitTermination())
            stream_query = storage.stream_to_delta(
                stream_df, stream_path, checkpoint_path
            )
            logger.info("Streaming query started")
            
            # Simula alguns micro-batches
            for _ in range(3):
                stream_query.processAllAvailable()
            
            # Para a query de streaming (apenas para demonstração)
            stream_query.stop()
            
            # 2. Monitoramento e observabilidade
            # Aqui você adicionaria validações com Great Expectations
            logger.info("Pipeline executed successfully")
            mlflow.log_metric("success", 1)
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            mlflow.log_metric("success", 0)
            raise

if __name__ == "__main__":
    run_pipeline()