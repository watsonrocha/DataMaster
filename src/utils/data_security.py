from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, sha2, lit, when
from pyspark.sql.types import StringType
import hashlib
import re
from typing import List, Dict

class DataSecurity:
    @staticmethod
    def encrypt_column(df: DataFrame, column: str, salt: str = "") -> DataFrame:
        """Criptografa uma coluna usando SHA-256"""
        return df.withColumn(column, sha2(col(column).cast("string"), 256))
    
    @staticmethod
    def mask_pii(df: DataFrame, columns: List[str], mask_char: str = "X") -> DataFrame:
        """Mascara dados PII (Informação Pessoal Identificável)"""
        for column in columns:
            df = df.withColumn(
                column,
                when(col(column).isNotNull(), 
                     udf(lambda x: mask_char * len(x) if x else None, StringType())(col(column)))
                .otherwise(col(column))
            )
        return df
    
    @staticmethod
    def anonymize_column(df: DataFrame, column: str) -> DataFrame:
        """Anonimiza uma coluna usando hash consistente"""
        def consistent_hash(value):
            if value is None:
                return None
            return hashlib.sha256(str(value).encode()).hexdigest()[:16]
            
        hash_udf = udf(consistent_hash, StringType())
        return df.withColumn(column, hash_udf(col(column)))
    
    @staticmethod
    def apply_access_control(df: DataFrame, user_roles: Dict[str, List[str]], sensitive_columns: Dict[str, List[str]]) -> DataFrame:
        """Aplica controle de acesso baseado em roles"""
        visible_columns = []
        for role in user_roles:
            if role in sensitive_columns:
                visible_columns.extend(sensitive_columns[role])
        
        # Remove colunas não autorizadas
        all_columns = set(df.columns)
        columns_to_select = [col for col in all_columns if col in visible_columns or col not in sum(sensitive_columns.values(), [])]
        
        return df.select(*columns_to_select)
    
    @staticmethod
    def pseudonymize_email(df: DataFrame, email_column: str, new_column: str = "pseudonymized_email") -> DataFrame:
        """Pseudonimiza endereços de email mantendo o domínio"""
        def pseudonymize(email):
            if not email:
                return None
            parts = email.split("@")
            if len(parts) == 2:
                return f"{hashlib.sha256(parts[0].encode()).hexdigest()[:8]}@{parts[1]}"
            return email
            
        pseudonymize_udf = udf(pseudonymize, StringType())
        return df.withColumn(new_column, pseudonymize_udf(col(email_column)))