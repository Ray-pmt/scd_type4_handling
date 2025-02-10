from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, to_date, current_timestamp, lit
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
import logging
from typing import Tuple, List, Dict
from dataclasses import dataclass
from datetime import datetime

@dataclass
class TableConfig:
    source_path: str
    current_path: str
    history_path: str
    hash_columns: List[str]
    hash_algorithm: int
    id_column: str
    effective_date_col: str
    current_effective_col: str
    partition_columns: List[str]
    required_columns: List[str]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CONFIG = TableConfig(
    source_path="/path/to/source/delta",
    current_path="/path/to/current/delta",
    history_path="/path/to/history/delta",
    hash_columns=["Name", "Salary", "OFFC"],
    hash_algorithm=256,
    id_column="ID",
    effective_date_col="Date_Eff",
    current_effective_col="Effective_Date",
    partition_columns=["ID"],  # Customize based on data distribution
    required_columns=["ID", "Name", "Salary", "OFFC", "Date_Eff"]
)

def create_spark_session(app_name: str = "SCD4_Production") -> SparkSession:
    """Create optimized Spark session with Delta Lake support."""
    return (SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "auto")
        .getOrCreate())

def validate_schema(df: DataFrame, required_cols: List[str]) -> bool:
    """Validate presence of required columns."""
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    return True

def get_change_hash(df: DataFrame, hash_cols: List[str], algorithm: int) -> DataFrame:
    """Generate optimized hash value for change detection."""
    # Convert all hash columns to string to ensure consistent hashing
    string_cols = [col(c).cast("string") for c in hash_cols]
    return df.withColumn("record_hash", 
        sha2(concat_ws("||", *string_cols), algorithm))

def load_data(spark: SparkSession, config: TableConfig) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Load and validate Delta tables with error handling."""
    try:
        # Validate source data
        source_df = spark.read.format("delta").load(config.source_path)
        validate_schema(source_df, config.required_columns)
        
        # Generate hash and optimize partitioning
        source_df = (get_change_hash(source_df, config.hash_columns, config.hash_algorithm)
            .repartition(*[col(c) for c in config.partition_columns]))
        
        # Load and validate current/history data
        current_df = spark.read.format("delta").load(config.current_path)
        history_df = spark.read.format("delta").load(config.history_path)
        
        return source_df, current_df, history_df
    
    except AnalysisException as e:
        logger.error(f"Failed to load Delta tables: {str(e)}")
        raise
    except ValueError as e:
        logger.error(f"Schema validation failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during data load: {str(e)}")
        raise

def process_scd4(
    spark: SparkSession,
    source_df: DataFrame,
    current_df: DataFrame,
    history_df: DataFrame,
    config: TableConfig
) -> Tuple[DataFrame, DataFrame]:
    """Process SCD type 4 with enhanced error handling and optimizations."""
    try:
        # Cache frequently used DataFrames
        source_df.cache()
        current_df.cache()
        
        # Create temporary views with explicit schema validation
        source_df.createOrReplaceTempView("source")
        current_df.createOrReplaceTempView("current")

        changes_df = spark.sql(f"""
            SELECT 
                curr.{config.id_column} as existing_id,
                src.*,
                curr.{config.current_effective_col} as current_effective_date,
                curr.record_hash as current_record_hash
            FROM source src
            LEFT JOIN current curr
                ON src.{config.id_column} = curr.{config.id_column}
            WHERE 
                curr.{config.id_column} IS NULL
                OR src.record_hash != curr.record_hash
        """)
        
        if changes_df.rdd.isEmpty():
            logger.info("No changes detected")
            return current_df, history_df

        num_changes = changes_df.count()
        logger.info(f"Detected {num_changes} changes")

        # Process historical records
        history_updates = (changes_df
            .filter(col("existing_id").isNotNull())
            .select(
                col(config.id_column),
                *[col(c) for c in config.hash_columns],
                col("current_effective_date").alias("From_date"),
                col(config.effective_date_col).alias("To_date"),
                col("current_record_hash").alias("record_hash"),
                current_timestamp().alias("processed_timestamp")
            ))

        if not history_updates.rdd.isEmpty():
            (history_updates.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .option("txnVersion", "1")
                .partitionBy(*config.partition_columns)
                .save(config.history_path))

        # Update current records using Delta Lake merge
        try:
            current_table = DeltaTable.forPath(spark, config.current_path)
            source_updates = (source_df
                .withColumnRenamed(config.effective_date_col, config.current_effective_col)
                .withColumn("processed_timestamp", current_timestamp()))

            (current_table.alias("curr")
                .merge(
                    source_updates.alias("src"),
                    f"curr.{config.id_column} = src.{config.id_column}"
                )
                .whenMatchedUpdate(
                    condition="curr.record_hash != src.record_hash",
                    set={
                        "Name": "src.Name",
                        "Salary": "src.Salary",
                        "OFFC": "src.OFFC",
                        config.current_effective_col: "src." + config.current_effective_col,
                        "record_hash": "src.record_hash",
                        "processed_timestamp": "src.processed_timestamp"
                    }
                )
                .whenNotMatchedInsertAll()
                .execute())

        except Exception as e:
            logger.error(f"Delta merge operation failed: {str(e)}")
            raise

        # Uncache DataFrames
        source_df.unpersist()
        current_df.unpersist()

        # Return updated data
        return (
            spark.read.format("delta").load(config.current_path),
            spark.read.format("delta").load(config.history_path)
        )

    except Exception as e:
        logger.error(f"SCD4 processing failed: {str(e)}")
        raise

def main():
    """Main execution flow with comprehensive error handling."""
    spark = None
    start_time = datetime.now()
    
    try:
        spark = create_spark_session()
        logger.info("Spark session created successfully")
        
        source_df, current_df, history_df = load_data(spark, CONFIG)
        logger.info("Data loaded and validated successfully")
        
        updated_current, updated_history = process_scd4(
            spark, source_df, current_df, history_df, CONFIG
        )
        
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Processing completed successfully in {processing_time} seconds")
        return updated_current, updated_history

    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
