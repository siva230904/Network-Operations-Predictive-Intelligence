# =========================================================
# DE6 — MySQL Warehouse Loader
# File: phase3/de6/warehouse_loader.py
# =========================================================

import json
import logging
from datetime import datetime
from pathlib import Path

import mysql.connector

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import (
    EXPECTED_GRID_COUNT,
    GEOJSON_PATH,
    HOURLY_GRID_SUMMARY_PATH,
    LOG_DIR,
    MYSQL_CONNECT_TIMEOUT,
    MYSQL_DATABASE,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_USER,
)


# =========================================================
# MySQL batch configuration
# =========================================================

# Number of fact rows sent to MySQL per transaction.
#
# The source contains approximately 1.5 million rows.
# We deliberately DO NOT collect all rows into Python
# memory at once.
#
# Instead:
#
# Spark
#   ↓
# toLocalIterator()
#   ↓
# 25,000 rows
#   ↓
# MySQL executemany()
#   ↓
# COMMIT
#   ↓
# next 25,000 rows
#
MYSQL_BATCH_SIZE = 25_000


# =========================================================
# Logging
# =========================================================

def create_logger():

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    log_file = (
        LOG_DIR
        / f"de6_warehouse_{timestamp}.log"
    )

    logger = logging.getLogger(
        f"DE6_Warehouse_{timestamp}"
    )

    logger.setLevel(
        logging.INFO
    )

    logger.propagate = False

    handler = logging.FileHandler(
        log_file,
        encoding="utf-8"
    )

    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        )
    )

    logger.addHandler(
        handler
    )

    return logger, log_file


# =========================================================
# MySQL connection
# =========================================================

def get_mysql_connection(
    database=True
):

    connection_args = {
        "host": MYSQL_HOST,
        "port": MYSQL_PORT,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "connection_timeout": MYSQL_CONNECT_TIMEOUT,
    }

    if database:

        connection_args[
            "database"
        ] = MYSQL_DATABASE

    return mysql.connector.connect(
        **connection_args
    )


# =========================================================
# Warehouse Loader
# =========================================================

class WarehouseLoader:

    REQUIRED_SOURCE_COLUMNS = [
        "timestamp",
        "grid_id",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity",
        "total_sms",
        "total_calls",
        "total_activity",
        "date",
        "hour",
        "day_of_week",
    ]

    # -----------------------------------------------------
    # Constructor
    # -----------------------------------------------------

    def __init__(
        self,
        spark,
        logger
    ):

        self.spark = spark

        self.logger = logger

        self.source_df = None

        self.dim_time_df = None

        self.dim_grid_df = None

        self.fact_df = None

        self.source_row_count = 0

        self.fact_row_count = 0

    # =====================================================
    # 1. Validate source paths
    # =====================================================

    def validate_paths(self):

        self.logger.info(
            "Validating DE6 input paths."
        )

        if not HOURLY_GRID_SUMMARY_PATH.exists():

            raise FileNotFoundError(
                "SP3 hourly_grid_summary Parquet "
                f"does not exist: "
                f"{HOURLY_GRID_SUMMARY_PATH}"
            )

        if not GEOJSON_PATH.exists():

            raise FileNotFoundError(
                "Milan grid GeoJSON does not exist: "
                f"{GEOJSON_PATH}"
            )

        self.logger.info(
            "SP3 Parquet: %s",
            HOURLY_GRID_SUMMARY_PATH
        )

        self.logger.info(
            "Reference GeoJSON: %s",
            GEOJSON_PATH
        )

    # =====================================================
    # 2. Read Spark source
    # =====================================================

    def read_source(self):

        self.logger.info(
            "Reading hourly_grid_summary Parquet."
        )

        df = (
            self.spark.read
            .parquet(
                str(
                    HOURLY_GRID_SUMMARY_PATH
                )
            )
        )

        missing = [
            column
            for column
            in self.REQUIRED_SOURCE_COLUMNS
            if column not in df.columns
        ]

        if missing:

            raise ValueError(
                "SP3 hourly_grid_summary is missing "
                f"required columns: {missing}"
            )

        self.source_df = df

        self.source_row_count = (
            df.count()
        )

        if self.source_row_count == 0:

            raise ValueError(
                "hourly_grid_summary contains zero rows."
            )

        self.logger.info(
            "Source rows: %d",
            self.source_row_count
        )

        return df

    # =====================================================
    # 3. Build dim_time
    # =====================================================

    def build_dim_time(self):

        if self.source_df is None:

            raise RuntimeError(
                "Read source first."
            )

        self.logger.info(
            "Building dim_time."
        )

        time_df = (
            self.source_df
            .select(
                "timestamp",
                "date",
                "hour",
                "day_of_week"
            )
            .distinct()
        )

        # -------------------------------------------------
        # time_key
        #
        # YYYYMMDDHH
        #
        # Example:
        #
        # 2013-11-01 05:00:00
        #
        # becomes:
        #
        # 2013110105
        # -------------------------------------------------

        time_df = (
            time_df
            .withColumn(
                "time_key",
                F.date_format(
                    "timestamp",
                    "yyyyMMddHH"
                )
                .cast("long")
            )
            .select(
                "time_key",
                "timestamp",
                "date",
                "hour",
                "day_of_week"
            )
        )

        duplicate_keys = (
            time_df
            .groupBy("time_key")
            .count()
            .filter(
                F.col("count") > 1
            )
            .count()
        )

        if duplicate_keys > 0:

            raise ValueError(
                "dim_time contains duplicate time keys."
            )

        self.dim_time_df = time_df

        count = (
            time_df.count()
        )

        self.logger.info(
            "dim_time rows: %d",
            count
        )

        return time_df

    # =====================================================
    # 4. Load GeoJSON
    # =====================================================

    def load_geojson(self):

        self.logger.info(
            "Loading static Milan GeoJSON."
        )

        with open(
            GEOJSON_PATH,
            "r",
            encoding="utf-8"
        ) as file:

            geojson = json.load(file)

        if geojson.get(
            "type"
        ) != "FeatureCollection":

            raise ValueError(
                "GeoJSON must be a FeatureCollection."
            )

        features = geojson.get(
            "features",
            []
        )

        if len(features) != EXPECTED_GRID_COUNT:

            raise ValueError(
                "Expected "
                f"{EXPECTED_GRID_COUNT} "
                "GeoJSON features, found "
                f"{len(features)}."
            )

        rows = []

        for feature_number, feature in enumerate(
            features,
            start=1
        ):

            properties = feature.get(
                "properties",
                {}
            )

            cell_id = properties.get(
                "cellId"
            )

            if cell_id is None:

                raise ValueError(
                    "Feature "
                    f"{feature_number} is missing "
                    "properties.cellId."
                )

            try:

                grid_id = int(
                    cell_id
                )

            except (
                TypeError,
                ValueError
            ):

                raise ValueError(
                    "Invalid cellId at feature "
                    f"{feature_number}: {cell_id}"
                )

            if not (
                1 <= grid_id <= EXPECTED_GRID_COUNT
            ):

                raise ValueError(
                    "Grid ID outside expected range "
                    f"1-{EXPECTED_GRID_COUNT}: "
                    f"{grid_id}"
                )

            geometry = feature.get(
                "geometry"
            )

            geometry_reference = None

            if geometry is not None:

                geometry_reference = json.dumps(
                    geometry,
                    separators=(",", ":")
                )

            # -------------------------------------------------
            # Calculate simple centroid from exterior ring.
            #
            # This is reference information only.
            # -------------------------------------------------

            latitude = None
            longitude = None

            centroid = self.geometry_centroid(
                geometry
            )

            if centroid is not None:

                longitude, latitude = centroid

            rows.append(
                (
                    grid_id,
                    latitude,
                    longitude,
                    geometry_reference
                )
            )

        return rows

    # =====================================================
    # 5. Geometry centroid helper
    # =====================================================

    @staticmethod
    def geometry_centroid(
        geometry
    ):

        if geometry is None:

            return None

        geometry_type = geometry.get(
            "type"
        )

        coordinates = geometry.get(
            "coordinates"
        )

        if not coordinates:

            return None

        if geometry_type == "Polygon":

            ring = coordinates[0]

        elif geometry_type == "MultiPolygon":

            ring = coordinates[0][0]

        else:

            return None

        if not ring:

            return None

        longitudes = []

        latitudes = []

        for point in ring:

            if len(point) < 2:

                continue

            longitudes.append(
                float(point[0])
            )

            latitudes.append(
                float(point[1])
            )

        if not longitudes:

            return None

        return (
            sum(longitudes)
            / len(longitudes),

            sum(latitudes)
            / len(latitudes)
        )

    # =====================================================
    # 6. Build dim_grid
    # =====================================================

    def build_dim_grid(self):

        self.logger.info(
            "Building dim_grid from static GeoJSON."
        )

        rows = self.load_geojson()

        self.dim_grid_df = (
            self.spark.createDataFrame(
                rows,
                [
                    "grid_id",
                    "latitude",
                    "longitude",
                    "geometry_reference",
                ]
            )
            .withColumn(
                "grid_key",
                F.col("grid_id")
            )
            .select(
                "grid_key",
                "grid_id",
                "latitude",
                "longitude",
                "geometry_reference",
            )
        )

        row_count = (
            self.dim_grid_df.count()
        )

        distinct_grid_ids = (
            self.dim_grid_df
            .select("grid_id")
            .distinct()
            .count()
        )

        if row_count != EXPECTED_GRID_COUNT:

            raise ValueError(
                "dim_grid must contain "
                f"{EXPECTED_GRID_COUNT} rows; "
                f"found {row_count}."
            )

        if distinct_grid_ids != row_count:

            raise ValueError(
                "dim_grid contains duplicate grid IDs."
            )

        self.logger.info(
            "dim_grid rows: %d",
            row_count
        )

        self.logger.info(
            "dim_grid distinct grid IDs: %d",
            distinct_grid_ids
        )

        return self.dim_grid_df

    # =====================================================
    # 7. Build fact table
    # =====================================================

    def build_fact(self):

        if self.source_df is None:

            raise RuntimeError(
                "Read source first."
            )

        if self.dim_time_df is None:

            raise RuntimeError(
                "Build dim_time first."
            )

        if self.dim_grid_df is None:

            raise RuntimeError(
                "Build dim_grid first."
            )

        self.logger.info(
            "Building fact_network_activity."
        )

        # -------------------------------------------------
        # Broadcast the small dimensions.
        #
        # dim_time and dim_grid are tiny compared with the
        # approximately 1.5M-row fact source.
        #
        # This avoids unnecessary large shuffle joins.
        # -------------------------------------------------

        fact = (
            self.source_df
            .join(
                F.broadcast(
                    self.dim_time_df.select(
                        "time_key",
                        "timestamp"
                    )
                ),
                on="timestamp",
                how="inner"
            )
            .join(
                F.broadcast(
                    self.dim_grid_df.select(
                        "grid_key",
                        "grid_id"
                    )
                ),
                on="grid_id",
                how="inner"
            )
        )

        self.fact_df = (
            fact
            .select(
                "time_key",
                "grid_key",
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "internet_activity",
                "total_sms",
                "total_calls",
                "total_activity",
            )
        )

        self.fact_row_count = (
            self.fact_df.count()
        )

        if (
            self.fact_row_count
            != self.source_row_count
        ):

            raise ValueError(
                "Fact row count does not match "
                "hourly_grid_summary. "
                f"Source={self.source_row_count}, "
                f"Fact={self.fact_row_count}. "
                "Possible dimension fan-out."
            )

        self.logger.info(
            "fact_network_activity rows: %d",
            self.fact_row_count
        )

        self.logger.info(
            "Fact row count matches Spark source."
        )

        return self.fact_df

    # =====================================================
    # 8. Create MySQL database
    # =====================================================

    def create_database(self):

        self.logger.info(
            "Creating MySQL database if necessary."
        )

        connection = get_mysql_connection(
            database=False
        )

        cursor = connection.cursor()

        try:

            cursor.execute(
                f"""
                CREATE DATABASE IF NOT EXISTS
                `{MYSQL_DATABASE}`
                """
            )

            connection.commit()

        finally:

            cursor.close()

            connection.close()

        self.logger.info(
            "MySQL database is ready: %s",
            MYSQL_DATABASE
        )

    # =====================================================
    # 9. Execute schema
    # =====================================================

    def execute_schema(self):

        self.logger.info(
            "Creating DE6 MySQL tables."
        )

        # -------------------------------------------------
        # IMPORTANT:
        #
        # __file__ is:
        #
        #   .../phase3/de6/warehouse_loader.py
        #
        # Therefore:
        #
        # Path(__file__).resolve()
        #
        # points to the FILE, not the directory.
        #
        # The previous version incorrectly did:
        #
        #   Path(__file__).resolve() / "mysql_schema.sql"
        #
        # which produced:
        #
        #   warehouse_loader.py/mysql_schema.sql
        #
        # Correct:
        #
        #   Path(__file__).resolve().parent
        # -------------------------------------------------

        schema_path = (
            Path(__file__).resolve().parent
            / "mysql_schema.sql"
        )

        if not schema_path.exists():

            raise FileNotFoundError(
                "MySQL schema file not found: "
                f"{schema_path}"
            )

        self.logger.info(
            "Schema file: %s",
            schema_path
        )

        with open(
            schema_path,
            "r",
            encoding="utf-8"
        ) as file:

            sql = file.read()

        connection = get_mysql_connection()

        cursor = connection.cursor()

        try:

            statements = [
                statement.strip()
                for statement in sql.split(";")
                if statement.strip()
            ]

            for statement in statements:

                cursor.execute(
                    statement
                )

            connection.commit()

        except Exception:

            connection.rollback()

            raise

        finally:

            cursor.close()

            connection.close()

        self.logger.info(
            "MySQL schema created successfully."
        )

    # =====================================================
    # 10. Load dim_time
    # =====================================================

    def load_dim_time(self):

        self.logger.info(
            "Loading dim_time into MySQL."
        )

        rows = (
            self.dim_time_df
            .orderBy("time_key")
            .collect()
        )

        connection = get_mysql_connection()

        cursor = connection.cursor()

        sql = """
            INSERT INTO dim_time (
                time_key,
                timestamp,
                date,
                hour,
                day_of_week
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s
            )
        """

        values = []

        try:

            for row in rows:

                timestamp_value = (
                    row["timestamp"].to_pydatetime()
                    if hasattr(
                        row["timestamp"],
                        "to_pydatetime"
                    )
                    else row["timestamp"]
                )

                values.append(
                    (
                        int(row["time_key"]),
                        timestamp_value,
                        row["date"],
                        int(row["hour"]),
                        int(row["day_of_week"]),
                    )
                )

            if values:

                cursor.executemany(
                    sql,
                    values
                )

                connection.commit()

        except Exception:

            connection.rollback()

            raise

        finally:

            cursor.close()

            connection.close()

        self.logger.info(
            "Loaded dim_time: %d rows.",
            len(values)
        )

    # =====================================================
    # 11. Load dim_grid
    # =====================================================

    def load_dim_grid(self):

        self.logger.info(
            "Loading dim_grid into MySQL."
        )

        rows = (
            self.dim_grid_df
            .orderBy("grid_key")
            .collect()
        )

        connection = get_mysql_connection()

        cursor = connection.cursor()

        sql = """
            INSERT INTO dim_grid (
                grid_key,
                grid_id,
                latitude,
                longitude,
                geometry_reference
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s
            )
        """

        values = []

        try:

            for row in rows:

                values.append(
                    (
                        int(row["grid_key"]),
                        int(row["grid_id"]),
                        row["latitude"],
                        row["longitude"],
                        row["geometry_reference"],
                    )
                )

            if values:

                cursor.executemany(
                    sql,
                    values
                )

                connection.commit()

        except Exception:

            connection.rollback()

            raise

        finally:

            cursor.close()

            connection.close()

        self.logger.info(
            "Loaded dim_grid: %d rows.",
            len(values)
        )

    # =====================================================
    # 12. Load fact in batches
    # =====================================================

    def load_fact(self):

        if self.fact_df is None:

            raise RuntimeError(
                "Build fact first."
            )

        self.logger.info(
            "Starting batch-wise MySQL fact load."
        )

        self.logger.info(
            "Fact rows to load: %d",
            self.fact_row_count
        )

        self.logger.info(
            "MySQL batch size: %d",
            MYSQL_BATCH_SIZE
        )

        sql = """
            INSERT INTO fact_network_activity (
                time_key,
                grid_key,
                sms_in,
                sms_out,
                call_in,
                call_out,
                internet_activity,
                total_sms,
                total_calls,
                total_activity
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
        """

        connection = get_mysql_connection()

        cursor = connection.cursor()

        batch = []

        total_loaded = 0

        batch_number = 0

        try:

            # -------------------------------------------------
            # DO NOT use:
            #
            # self.fact_df.rdd.mapPartitions(...)
            #
            # That approach caused the Spark Python-worker
            # connection timeout on the Windows environment.
            #
            # toLocalIterator() streams rows from the Spark
            # DataFrame without explicitly converting it into
            # an RDD.
            # -------------------------------------------------

            iterator = (
                self.fact_df
                .toLocalIterator()
            )

            for row in iterator:

                batch.append(
                    (
                        int(row["time_key"]),
                        int(row["grid_key"]),

                        float(row["sms_in"]),
                        float(row["sms_out"]),

                        float(row["call_in"]),
                        float(row["call_out"]),

                        float(
                            row["internet_activity"]
                        ),

                        float(
                            row["total_sms"]
                        ),

                        float(
                            row["total_calls"]
                        ),

                        float(
                            row["total_activity"]
                        ),
                    )
                )

                # -------------------------------------------------
                # Commit every MYSQL_BATCH_SIZE rows.
                # -------------------------------------------------

                if len(batch) >= MYSQL_BATCH_SIZE:

                    cursor.executemany(
                        sql,
                        batch
                    )

                    connection.commit()

                    batch_number += 1

                    loaded_now = len(batch)

                    total_loaded += loaded_now

                    self.logger.info(
                        "Fact batch %d committed: "
                        "%d rows | "
                        "total=%d/%d "
                        "(%.2f%%)",
                        batch_number,
                        loaded_now,
                        total_loaded,
                        self.fact_row_count,
                        (
                            total_loaded
                            / self.fact_row_count
                            * 100
                        )
                        if self.fact_row_count
                        else 100.0
                    )

                    batch.clear()

            # -------------------------------------------------
            # Final partial batch.
            # -------------------------------------------------

            if batch:

                cursor.executemany(
                    sql,
                    batch
                )

                connection.commit()

                batch_number += 1

                loaded_now = len(batch)

                total_loaded += loaded_now

                self.logger.info(
                    "Fact final batch %d committed: "
                    "%d rows | "
                    "total=%d/%d "
                    "(%.2f%%)",
                    batch_number,
                    loaded_now,
                    total_loaded,
                    self.fact_row_count,
                    (
                        total_loaded
                        / self.fact_row_count
                        * 100
                    )
                    if self.fact_row_count
                    else 100.0
                )

                batch.clear()

        except Exception:

            connection.rollback()

            self.logger.exception(
                "Fact batch load failed. "
                "Last committed rows: %d",
                total_loaded
            )

            raise

        finally:

            cursor.close()

            connection.close()

        # -----------------------------------------------------
        # Final Python-side validation.
        # -----------------------------------------------------

        if (
            total_loaded
            != self.fact_row_count
        ):

            raise ValueError(
                "Fact MySQL load count does not match "
                "Spark fact count. "
                f"Expected={self.fact_row_count}, "
                f"Loaded={total_loaded}"
            )

        self.logger.info(
            "Fact load completed successfully."
        )

        self.logger.info(
            "Total fact rows loaded: %d",
            total_loaded
        )

        self.logger.info(
            "Total MySQL batches: %d",
            batch_number
        )

    # =====================================================
    # 13. Verify MySQL counts
    # =====================================================

    def verify_counts(self):

        self.logger.info(
            "Verifying MySQL warehouse counts."
        )

        connection = get_mysql_connection()

        cursor = connection.cursor()

        try:

            cursor.execute(
                "SELECT COUNT(*) FROM dim_time"
            )

            dim_time_count = (
                cursor.fetchone()[0]
            )

            cursor.execute(
                "SELECT COUNT(*) FROM dim_grid"
            )

            dim_grid_count = (
                cursor.fetchone()[0]
            )

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM fact_network_activity
                """
            )

            fact_count = (
                cursor.fetchone()[0]
            )

        finally:

            cursor.close()

            connection.close()

        self.logger.info(
            "MySQL dim_time rows: %d",
            dim_time_count
        )

        self.logger.info(
            "MySQL dim_grid rows: %d",
            dim_grid_count
        )

        self.logger.info(
            "MySQL fact rows: %d",
            fact_count
        )

        if (
            dim_grid_count
            != EXPECTED_GRID_COUNT
        ):

            raise ValueError(
                "dim_grid row count mismatch. "
                f"Expected={EXPECTED_GRID_COUNT}, "
                f"Actual={dim_grid_count}"
            )

        if (
            fact_count
            != self.source_row_count
        ):

            raise ValueError(
                "Fact row count mismatch. "
                f"Source={self.source_row_count}, "
                f"MySQL fact={fact_count}"
            )

        return {
            "dim_time": dim_time_count,
            "dim_grid": dim_grid_count,
            "fact": fact_count,
        }

    # =====================================================
    # 14. Full load
    # =====================================================

    def run(self):

        # -------------------------------------------------
        # Source validation
        # -------------------------------------------------

        self.validate_paths()

        # -------------------------------------------------
        # Read source
        # -------------------------------------------------

        self.read_source()

        # -------------------------------------------------
        # Build dimensions
        # -------------------------------------------------

        self.build_dim_time()

        self.build_dim_grid()

        # -------------------------------------------------
        # Build fact
        # -------------------------------------------------

        self.build_fact()

        # -------------------------------------------------
        # MySQL preparation
        # -------------------------------------------------

        self.create_database()

        self.execute_schema()

        # -------------------------------------------------
        # Load dimensions first.
        #
        # This is required because the fact table contains
        # foreign keys referencing both dimensions.
        # -------------------------------------------------

        self.load_dim_time()

        self.load_dim_grid()

        # -------------------------------------------------
        # Load approximately 1.5M fact rows in batches.
        # -------------------------------------------------

        self.load_fact()

        # -------------------------------------------------
        # Verify final MySQL state.
        # -------------------------------------------------

        counts = (
            self.verify_counts()
        )

        self.logger.info(
            "DE6 warehouse load completed successfully."
        )

        return counts


# =========================================================
# Main
# =========================================================

def main():

    logger, log_file = (
        create_logger()
    )

    start_time = datetime.now()

    logger.info(
        "DE6 START"
    )

    logger.info(
        "Start time: %s",
        start_time.isoformat()
    )

    spark = None

    try:

        # -------------------------------------------------
        # Spark configuration
        # -------------------------------------------------

        spark = (
            SparkSession.builder
            .master("local[2]")
            .config(
                "spark.python.worker.reuse",
                "true"
            )
            .config(
                "spark.sql.shuffle.partitions",
                "4"
            )
            .appName(
                "DE6_MySQL_Warehouse"
            )
            .getOrCreate()
        )

        logger.info(
            "Spark session created."
        )

        loader = WarehouseLoader(
            spark=spark,
            logger=logger
        )

        counts = (
            loader.run()
        )

        end_time = datetime.now()

        logger.info(
            "DE6 STATUS: SUCCESS"
        )

        logger.info(
            "End time: %s",
            end_time.isoformat()
        )

        logger.info(
            "Log file: %s",
            log_file
        )

        print()
        print("=" * 70)
        print("DE6 WAREHOUSE LOAD SUCCESS")
        print("=" * 70)

        print(
            f"dim_time rows : "
            f"{counts['dim_time']:,}"
        )

        print(
            f"dim_grid rows : "
            f"{counts['dim_grid']:,}"
        )

        print(
            f"fact rows     : "
            f"{counts['fact']:,}"
        )

        print(
            f"Log           : "
            f"{log_file}"
        )

        print("=" * 70)

        return 0

    except Exception as exc:

        end_time = datetime.now()

        logger.exception(
            "DE6 STATUS: FAILED"
        )

        logger.error(
            "Failure reason: %s",
            exc
        )

        logger.info(
            "End time: %s",
            end_time.isoformat()
        )

        print()
        print("=" * 70)
        print("DE6 WAREHOUSE LOAD FAILED")
        print("=" * 70)

        print(
            f"Reason: {exc}"
        )

        print(
            f"Log: {log_file}"
        )

        print("=" * 70)

        return 1

    finally:

        if spark is not None:

            try:

                spark.stop()

            except Exception:

                logger.exception(
                    "Error while stopping Spark."
                )


# =========================================================
# Entry point
# =========================================================

if __name__ == "__main__":

    raise SystemExit(
        main()
    )

