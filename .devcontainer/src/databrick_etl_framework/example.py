if __name__ == "__main__":
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    # Example 1: Azure → Delta (Auto Loader, external table)
    azure_cfg = IngestionConfig(
        source_path="abfss://raw@myacct.dfs.core.windows.net/sales/2025/",
        source_format="json",
        source_options={"multiLine": "true"},
        catalog="hive_metastore",  # or your UC catalog like "corp"
        schema="bronze",
        table="sales_events_raw",
        target_path="abfss://delta@myacct.dfs.core.windows.net/tables/bronze/sales_events_raw",
        write_mode="append",
        ingest_mode="autoloader",
        checkpoint_path="abfss://ckpt@myacct.dfs.core.windows.net/_ckpt/sales_events_raw/",
        schema_infer=True,
        schema_path="abfss://schemas@myacct.dfs.core.windows.net/autoloader/sales_events_raw/",
        partition_by=["event_date"],
        zorder_by=["customer_id", "event_date"],
        table_properties={"delta.autoOptimize.optimizeWrite": "true", "delta.autoOptimize.autoCompact": "true"},
        description="Raw sales events ingested via Auto Loader from ADLS Gen2",
        dry_run=False,
    )
    azure_job = make_ingestion(spark, azure_cfg)
    # azure_job.run()

    # Example 2: S3 → Delta (Batch, managed UC table)
    s3_cfg = IngestionConfig(
        source_path="s3://my-raw-bucket/sensors/",
        source_format="parquet",
        catalog="corp",
        schema="silver",
        table="sensor_readings",
        target_path=None,  # managed table
        write_mode="merge",
        ingest_mode="batch",
        merge_keys=["device_id", "reading_ts"],
        partition_by=["reading_date"],
        zorder_by=["device_id", "reading_ts"],
        table_properties={"delta.enableChangeDataFeed": "true"},
        description="Curated sensor readings with upserts from S3 parquet",
    )
    s3_job = make_ingestion(spark, s3_cfg)
    # s3_job.run()

    print("Loaded framework. Uncomment .run() calls to execute in your Databricks notebook/Job.")
