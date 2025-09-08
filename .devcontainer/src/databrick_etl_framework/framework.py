"""
Databricks Object-Oriented Ingestion Framework
=============================================

Goal
----
A single, parameter-driven framework to ingest files from **Azure ADLS/Blob** or **AWS S3**
into **Delta Lake**, and materialize a **visible Unity Catalog table** in the requested
catalog & schema. Supports both **batch** and **Auto Loader (streaming)** modes.

Key Features
------------
- Unified config object (`IngestionConfig`) with strong typing and sensible defaults
- Cloud-agnostic base class with cloud-specific specializations (Azure/S3)
- Batch or Auto Loader (streaming) ingestion with schema evolution
- Optional table partitioning & Z-Ordering
- Safe table creation via `CREATE TABLE ... USING DELTA LOCATION` (external) or `saveAsTable` (managed)
- Dry-run planning and idempotent create/merge of UC objects
- Minimal dependencies; pure PySpark/Delta

Usage
-----
See examples at the bottom of this file for **Azure** and **S3**.

Notes
-----
- Assumes your workspace has UC enabled and, for external tables, the location is accessible by a
  configured **Storage Credential / External Location**. If not, you can still write to a managed
  table without specifying `target_path`.
- For Auto Loader on S3, you must have the `cloudFiles` libraries (native in Databricks runtime) and
  configure S3 permissions (instance profile, IAM role, or credentials passthrough) accordingly.
- Schema inference for production should be provided explicitly to avoid drift.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Literal
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame

WriteMode = Literal["append", "overwrite", "merge"]
IngestMode = Literal["batch", "autoloader"]


@dataclass
class IngestionConfig:
    # Source
    source_path: str                      # e.g., "abfss://container@acct.dfs.core.windows.net/path" or "s3://bucket/prefix"
    source_format: str = "json"           # csv|json|parquet|... (for Auto Loader, set cloudFiles.format)
    source_options: Dict[str, str] = field(default_factory=dict)

    # Destination (Delta)
    catalog: str = "main"                 # UC catalog
    schema: str = "default"               # UC schema
    table: str = "my_table"               # UC table name
    target_path: Optional[str] = None     # If set, creates an EXTERNAL delta table at this path; else managed table

    # Behavior
    write_mode: WriteMode = "append"      # append|overwrite|merge (merge requires keys)
    ingest_mode: IngestMode = "batch"     # batch|autoloader
    checkpoint_path: Optional[str] = None # required for autoloader

    # Schema & merge behavior
    schema_infer: bool = True
    schema_path: Optional[str] = None     # optional cloud path to store inferred schema for autoloader
    merge_keys: Optional[List[str]] = None # required if write_mode == "merge"

    # Performance & layout
    partition_by: Optional[List[str]] = None
    zorder_by: Optional[List[str]] = None

    # Misc
    table_properties: Dict[str, str] = field(default_factory=dict)
    description: Optional[str] = None

    # Safety/Dry run
    dry_run: bool = False

    def full_table_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


class IngestionError(Exception):
    pass


class BaseIngestion(ABC):
    def __init__(self, spark: SparkSession, cfg: IngestionConfig):
        self.spark = spark
        self.cfg = cfg

    # --------------------------- Public API --------------------------- #
    def run(self) -> None:
        self._validate()
        plan = self._plan()
        if self.cfg.dry_run:
            print("[DRY RUN] Ingestion plan:\n" + plan)
            return

        self._ensure_catalog_and_schema()

        if self.cfg.ingest_mode == "batch":
            df = self._read_batch()
            self._write_batch(df)
        else:
            self._write_stream()

        self._optimize_post_write()

    # ---------------------- Abstract / cloud bits --------------------- #
    @abstractmethod
    def _add_auth(self, reader_or_writer):
        """Hook for cloud auth (if needed). Typically no-op on Databricks with workspace-configured auth."""
        raise NotImplementedError

    @abstractmethod
    def _validate_source_uri(self) -> None:
        raise NotImplementedError

    # -------------------------- Core helpers -------------------------- #
    def _validate(self) -> None:
        if self.cfg.ingest_mode == "autoloader" and not self.cfg.checkpoint_path:
            raise IngestionError("checkpoint_path is required for autoloader mode")
        if self.cfg.write_mode == "merge" and not self.cfg.merge_keys:
            raise IngestionError("merge_keys are required when write_mode='merge'")
        self._validate_source_uri()

    def _plan(self) -> str:
        return (
            f"Source: {self.cfg.source_path} (format={self.cfg.source_format})\n"
            f"Dest UC Table: {self.cfg.full_table_name()}\n"
            f"Dest Path: {self.cfg.target_path or '[managed table]'}\n"
            f"Mode: {self.cfg.ingest_mode}/{self.cfg.write_mode}\n"
            f"Options: {self.cfg.source_options}\n"
            f"Partition By: {self.cfg.partition_by}\n"
            f"Z-Order By: {self.cfg.zorder_by}\n"
        )

    def _ensure_catalog_and_schema(self) -> None:
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.cfg.catalog}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.cfg.catalog}.{self.cfg.schema}")

    # ----------------------------- Read ------------------------------- #
    def _read_batch(self) -> DataFrame:
        reader = self.spark.read
        if self.cfg.schema_infer:
            reader = reader.option("inferSchema", "true")
        for k, v in self.cfg.source_options.items():
            reader = reader.option(k, v)
        self._add_auth(reader)
        return reader.format(self.cfg.source_format).load(self.cfg.source_path)

    # ---------------------------- Write ------------------------------- #
    def _write_batch(self, df: DataFrame) -> None:
        target = self.cfg.full_table_name()

        if self.cfg.write_mode == "merge":
            self._merge_into(df)
            return

        writer = df.write.format("delta")
        if self.cfg.partition_by:
            writer = writer.partitionBy(*self.cfg.partition_by)

        if self.cfg.target_path:
            writer = writer.mode(self.cfg.write_mode)
            writer.save(self.cfg.target_path)
            # Make the table visible in UC (external table at LOCATION)
            self._create_table_if_not_exists_external()
        else:
            # Managed table; saveAsTable will register UC table
            writer = writer.mode(self.cfg.write_mode)
            writer.saveAsTable(target)

        self._apply_table_metadata()

    def _write_stream(self) -> None:
        target = self.cfg.full_table_name()
        reader = (self.spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", self.cfg.source_format))

        # schema management
        if self.cfg.schema_path:
            reader = reader.option("cloudFiles.schemaLocation", self.cfg.schema_path)
        if self.cfg.schema_infer:
            reader = reader.option("cloudFiles.inferColumnTypes", "true")

        for k, v in self.cfg.source_options.items():
            reader = reader.option(k, v)

        self._add_auth(reader)
        df = reader.load(self.cfg.source_path)

        writer = (df.writeStream
                    .format("delta")
                    .outputMode("append")
                    .option("checkpointLocation", self.cfg.checkpoint_path))

        if self.cfg.target_path:
            query = writer.start(self.cfg.target_path)
            query.processAllAvailable()
            # Register external table
            self._create_table_if_not_exists_external()
        else:
            query = writer.toTable(target)
            query.processAllAvailable()

        self._apply_table_metadata()

    def _merge_into(self, df: DataFrame) -> None:
        from delta.tables import DeltaTable

        if self.cfg.target_path:
            target_exists = self._delta_exists_at_path(self.cfg.target_path)
            if not target_exists:
                # Initialize empty Delta at path for external table
                (df.limit(0).write.format("delta").mode("overwrite").save(self.cfg.target_path))
                self._create_table_if_not_exists_external()
            delta_tbl = DeltaTable.forPath(self.spark, self.cfg.target_path)
        else:
            full = self.cfg.full_table_name()
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {full} USING DELTA")
            delta_tbl = DeltaTable.forName(self.spark, full)

        on_expr = " AND ".join([f"t.{k} = s.{k}" for k in self.cfg.merge_keys])
        (delta_tbl.alias("t")
            .merge(df.alias("s"), on_expr)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())

    def _delta_exists_at_path(self, path: str) -> bool:
        try:
            return self.spark._jsparkSession.sessionState().catalog().tableExists(path) or \
                   len(self.spark.read.format("delta").load(path).schema.names) >= 0
        except Exception:
            return False

    def _create_table_if_not_exists_external(self) -> None:
        full = self.cfg.full_table_name()
        loc = self.cfg.target_path
        assert loc, "target_path required for external table registration"
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {full} USING DELTA LOCATION '{loc}'"
        )

    def _apply_table_metadata(self) -> None:
        full = self.cfg.full_table_name()
        if self.cfg.description:
            self.spark.sql(f"COMMENT ON TABLE {full} IS '{self.cfg.description.replace("'", "''")}'")
        if self.cfg.table_properties:
            props = ", ".join([f"{k}='{v}'" for k, v in self.cfg.table_properties.items()])
            self.spark.sql(f"ALTER TABLE {full} SET TBLPROPERTIES ({props})")

    def _optimize_post_write(self) -> None:
        full = self.cfg.full_table_name()
        # OPTIMIZE + Z-ORDER (if available in your workspace/edition)
        try:
            if self.cfg.zorder_by:
                cols = ", ".join(self.cfg.zorder_by)
                self.spark.sql(f"OPTIMIZE {full} ZORDER BY ({cols})")
            else:
                self.spark.sql(f"OPTIMIZE {full}")
        except Exception as e:
            print(f"[WARN] OPTIMIZE failed or not supported: {e}")


# -------------------------- Cloud Specializations -------------------------- #

class AzureIngestion(BaseIngestion):
    def _add_auth(self, reader_or_writer):
        # Typically handled by cluster/workspace configs (Azure MSI/Service Principal w/ OAuth).
        return reader_or_writer

    def _validate_source_uri(self) -> None:
        if not self.cfg.source_path.startswith(("abfss://", "wasbs://", "adl://", "abfs://")):
            raise IngestionError("Azure source_path must start with abfss://, abfs://, wasbs://, or adl://")
        if self.cfg.target_path and not self.cfg.target_path.startswith(("abfss://", "abfs://")):
            raise IngestionError("Azure target_path must start with abfss:// or abfs:// for external tables")


class S3Ingestion(BaseIngestion):
    def _add_auth(self, reader_or_writer):
        # Usually IAM role/instance profile or credentials passthrough; nothing to set here.
        return reader_or_writer

    def _validate_source_uri(self) -> None:
        if not self.cfg.source_path.startswith("s3://"):
            raise IngestionError("S3 source_path must start with s3://")
        if self.cfg.target_path and not self.cfg.target_path.startswith("s3://"):
            raise IngestionError("S3 target_path must start with s3:// for external tables")


# ------------------------------- Factory ---------------------------------- #

def make_ingestion(spark: SparkSession, cfg: IngestionConfig) -> BaseIngestion:
    if cfg.source_path.startswith(("abfss://", "abfs://", "wasbs://", "adl://")):
        return AzureIngestion(spark, cfg)
    if cfg.source_path.startswith("s3://"):
        return S3Ingestion(spark, cfg)
    raise IngestionError("Unsupported source URI scheme; expected Azure (abfss/abfs/wasbs/adl) or S3 (s3)")


# ------------------------------- Examples ---------------------------------- #
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
