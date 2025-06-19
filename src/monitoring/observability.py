from pyspark.sql import SparkSession, DataFrame
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import SparkDFDataset
import mlflow
from datetime import datetime
import json
import logging

class DataObservability:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        
    def log_data_quality(self, df: DataFrame, expectation_suite: ExpectationSuite):
        """Avalia a qualidade dos dados com Great Expectations"""
        ge_df = SparkDFDataset(df)
        results = ge_df.validate(expectation_suite=expectation_suite)
        
        # Log para MLflow
        with mlflow.start_run():
            mlflow.log_metric("success_percent", results["statistics"]["success_percent"])
            mlflow.log_text(json.dumps(results["results"]), "validation_results.json")
            
        return results
    
    def monitor_performance(self, func, *args, **kwargs):
        """Decorador para monitorar performance de funções"""
        start_time = datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        self.logger.info(f"Function {func.__name__} took {duration:.2f} seconds")
        
        # Log para MLflow
        if mlflow.active_run():
            mlflow.log_metric(f"{func.__name__}_duration", duration)
        
        return result
    
    def track_data_lineage(self, df: DataFrame, operation: str, metadata: dict = None):
        """Rastreia a linhagem de dados"""
        lineage_data = {
            "timestamp": datetime.now().isoformat(),
            "operation": operation,
            "columns": df.columns,
            "count": df.count(),
            "metadata": metadata or {}
        }
        
        self.logger.info(f"Data Lineage: {json.dumps(lineage_data)}")
        
        # Log para MLflow
        if mlflow.active_run():
            mlflow.log_dict(lineage_data, f"lineage/{operation}_{datetime.now().timestamp()}.json")
    
    def alert_anomalies(self, metrics: dict, thresholds: dict):
        """Gera alertas com base em métricas e thresholds"""
        alerts = []
        for metric, value in metrics.items():
            threshold = thresholds.get(metric)
            if threshold:
                if value > threshold.get("max"):
                    alerts.append(f"{metric} above maximum threshold ({value} > {threshold['max']})")
                elif value < threshold.get("min"):
                    alerts.append(f"{metric} below minimum threshold ({value} < {threshold['min']})")
        
        if alerts:
            self.logger.warning("Data anomalies detected:\n" + "\n".join(alerts))
            return alerts
        return None