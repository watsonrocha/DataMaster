import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Configurações do Spark
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
    SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "PySparkDataPipeline")
    
    # Configurações de armazenamento
    DATA_LAKE_PATH = os.getenv("DATA_LAKE_PATH", "data/lake")
    CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "data/checkpoints")
    
    # Configurações de segurança
    ENCRYPTION_SALT = os.getenv("ENCRYPTION_SALT", "default-salt-value")
    
    # Configurações de Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "data-ingestion-topic")
    
    # Configurações de banco de dados
    DB_JDBC_URL = os.getenv("DB_JDBC_URL", "jdbc:postgresql://localhost:5432/mydb")
    DB_USER = os.getenv("DB_USER", "user")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "password")