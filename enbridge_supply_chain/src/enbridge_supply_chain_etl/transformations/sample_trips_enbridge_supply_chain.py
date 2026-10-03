# =============================================================================
# Bronze Layer: Raw data ingestion for Enbridge SCM
# Ingests purchase orders and supplier data into streaming tables
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp


@dp.table(
    comment="Raw purchase order data ingested from source systems"
)
def bronze_purchase_orders():
    """Ingest raw purchase order data.
    In production, this would use Auto Loader to read from cloud storage.
    For this sample, we read from the existing source table.
    """
    source_catalog = spark.conf.get("source_catalog")
    source_schema = spark.conf.get("source_schema")
    return (
        spark.read.table(f"{source_catalog}.{source_schema}.bronze_purchase_orders")
        .withColumn("_ingested_at", current_timestamp())
    )


@dp.table(
    comment="Raw supplier dimension data"
)
def bronze_suppliers():
    """Ingest supplier dimension data."""
    source_catalog = spark.conf.get("source_catalog")
    source_schema = spark.conf.get("source_schema")
    return (
        spark.read.table(f"{source_catalog}.{source_schema}.bronze_suppliers")
        .withColumn("_ingested_at", current_timestamp())
    )
