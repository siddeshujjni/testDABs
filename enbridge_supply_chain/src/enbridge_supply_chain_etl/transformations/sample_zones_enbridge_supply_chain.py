# =============================================================================
# Silver & Gold Layers: Enrichment and aggregation for Enbridge SCM
# Applies business rules and creates analytics-ready tables
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, datediff, when, avg, count, sum as _sum,
    round as _round, current_timestamp
)


# ---- SILVER LAYER ----

@dp.table(
    comment="Enriched purchase orders with supplier info and lead time calculations"
)
def silver_purchase_orders_enriched():
    """Join POs with suppliers and calculate lead time metrics.
    Applies Enbridge Business Rules R1-R3.
    """
    po = dp.read("bronze_purchase_orders")
    suppliers = dp.read("bronze_suppliers")

    return (
        po.join(suppliers, po.supplier_id == suppliers.supplier_id, "left")
        .withColumn(
            "lead_time_days",
            datediff(col("received_date"), col("order_date"))
        )
        .withColumn(
            "is_delayed",
            when(col("lead_time_days") > 14, 1).otherwise(0)  # R1: >14 days = delayed
        )
        .withColumn(
            "delay_risk_score",
            when(col("lead_time_days") > 30, 1.0)              # R2: Risk scoring
            .when(col("lead_time_days") > 21, 0.75)
            .when(col("lead_time_days") > 14, 0.5)
            .when(col("lead_time_days") > 7, 0.25)
            .otherwise(0.0)
        )
        .withColumn(
            "risk_category",
            when(col("delay_risk_score") >= 0.75, "High")       # R3: Risk categories
            .when(col("delay_risk_score") >= 0.5, "Medium")
            .when(col("delay_risk_score") >= 0.25, "Low")
            .otherwise("Minimal")
        )
        .drop(suppliers.supplier_id)
    )


# ---- GOLD LAYER ----

@dp.table(
    comment="Gold: PO delay risk scores for downstream analytics and dashboards"
)
def gold_po_delay_risk_scored():
    """Final enriched PO table with all risk metrics."""
    return dp.read("silver_purchase_orders_enriched").select(
        "po_id", "supplier_id", "supplier_name", "region", "country",
        "product_category", "order_date", "received_date", "status",
        "quantity", "unit_cost", "total_cost",
        "lead_time_days", "is_delayed", "delay_risk_score", "risk_category"
    )


@dp.table(
    comment="Gold: Supplier-level lead time aggregates and performance metrics"
)
def gold_supplier_lead_time():
    """Aggregate supplier performance metrics.
    Applies Business Rule R4: Supplier scorecarding.
    """
    enriched = dp.read("silver_purchase_orders_enriched")

    return (
        enriched
        .groupBy("supplier_id", "supplier_name", "region", "country")
        .agg(
            _round(avg("lead_time_days"), 1).alias("avg_lead_time_days"),
            count("po_id").alias("num_orders"),
            _round(_sum("total_cost"), 2).alias("total_spend"),
            _round(avg("delay_risk_score"), 3).alias("avg_risk_score"),
            _sum("is_delayed").alias("delayed_orders")
        )
        .withColumn(
            "on_time_rate",
            _round(1.0 - (col("delayed_orders") / col("num_orders")), 3)
        )
    )
