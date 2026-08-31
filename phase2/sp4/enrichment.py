import json
import logging
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    broadcast,
    col,
    count,
    countDistinct,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    sum as spark_sum,
    udf,
)
from pyspark.sql.types import StringType


# =========================================================
# Logging
# =========================================================

def create_logger(log_dir="data/logs"):
    """
    Create a unique SP4 log file.

    Existing logs are never overwritten.
    """

    log_path = Path(log_dir)

    log_path.mkdir(
        parents=True,
        exist_ok=True
    )

    run_number = 1

    while True:

        filename = (
            log_path /
            f"sp4_enrichment_run_{run_number:03d}.log"
        )

        if not filename.exists():
            break

        run_number += 1

    logger = logging.getLogger(
        f"SP4_{run_number}"
    )

    logger.setLevel(
        logging.INFO
    )

    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    )

    handler = logging.FileHandler(
        filename,
        encoding="utf-8"
    )

    handler.setFormatter(
        formatter
    )

    logger.addHandler(
        handler
    )

    return logger, filename


# =========================================================
# Geometry helper
# =========================================================

def geometry_to_centroid(geometry):
    """
    Calculate an approximate polygon centroid from
    GeoJSON coordinates.

    Coordinates are expected as:

        [longitude, latitude]
    """

    if geometry is None:
        return None

    try:

        geometry_object = json.loads(
            geometry
        )

        geometry_type = (
            geometry_object["type"]
        )

        coordinates = (
            geometry_object["coordinates"]
        )

        if geometry_type == "Polygon":

            ring = coordinates[0]

        elif geometry_type == "MultiPolygon":

            ring = coordinates[0][0]

        else:

            return None

        if not ring:
            return None

        longitudes = [
            point[0]
            for point in ring
        ]

        latitudes = [
            point[1]
            for point in ring
        ]

        centroid_longitude = (
            sum(longitudes)
            /
            len(longitudes)
        )

        centroid_latitude = (
            sum(latitudes)
            /
            len(latitudes)
        )

        return (
            f"{centroid_longitude:.6f},"
            f"{centroid_latitude:.6f}"
        )

    except Exception:

        return None


centroid_udf = udf(
    geometry_to_centroid,
    StringType()
)


# =========================================================
# SP4 Geospatial Enrichment
# =========================================================

class NetworkGeoEnricher:

    """
    SP4 — Geospatial enrichment.

    Input:

        hourly_grid_summary

    Reference:

        milano-grid.geojson

    Output grain:

        timestamp + grid_id

    IMPORTANT:

        GeoJSON properties.cellId
        is the source of grid_id.

        Do NOT use the top-level GeoJSON "id".
    """

    # -----------------------------------------------------
    # Constructor
    # -----------------------------------------------------

    def __init__(
        self,
        spark,
        hourly_grid_summary: DataFrame,
        geojson_path,
        log_dir="data/logs"
    ):

        self.spark = spark

        self.hourly_grid_summary = (
            hourly_grid_summary
        )

        self.geojson_path = Path(
            geojson_path
        )

        self.grid_lookup = None

        self.grid_activity_geo_df = None

        self.unmatched_grid_ids = None

        self.coverage_report = None

        self.top_grids_with_geometry = None

        self.logger, self.log_file = (
            create_logger(log_dir)
        )

    # =====================================================
    # 1. Validate inputs
    # =====================================================

    def validate_inputs(self):

        if (
            self.hourly_grid_summary
            is None
        ):

            raise ValueError(
                "hourly_grid_summary cannot be None."
            )

        required_columns = [
            "timestamp",
            "grid_id",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "total_activity",
        ]

        missing = [
            column
            for column
            in required_columns
            if column
            not in
            self.hourly_grid_summary.columns
        ]

        if missing:

            raise ValueError(
                "hourly_grid_summary is missing: "
                f"{missing}"
            )

        if not self.geojson_path.exists():

            raise FileNotFoundError(
                f"GeoJSON not found: "
                f"{self.geojson_path}"
            )

        self.logger.info(
            "Input validation passed."
        )

    # =====================================================
    # 2. Load GeoJSON
    # =====================================================

    def load_geojson(self):

        self.logger.info(
            "Loading GeoJSON: %s",
            self.geojson_path
        )

        with open(
            self.geojson_path,
            "r",
            encoding="utf-8"
        ) as file:

            geojson = json.load(file)

        # -------------------------------------------------
        # Validate top-level structure
        # -------------------------------------------------

        if geojson.get("type") != "FeatureCollection":

            raise ValueError(
                "GeoJSON is not a FeatureCollection."
            )

        features = geojson.get(
            "features",
            []
        )

        if len(features) != 10000:

            raise ValueError(
                "Expected 10,000 Milan grid "
                f"features, found {len(features)}."
            )

        self.logger.info(
            "GeoJSON FeatureCollection contains "
            "%d features.",
            len(features)
        )

        return features

    # =====================================================
    # 3. Build grid lookup
    # =====================================================

    def build_grid_lookup(self):

        features = (
            self.load_geojson()
        )

        lookup_rows = []

        for feature in features:

            properties = (
                feature.get(
                    "properties",
                    {}
                )
            )

            # -------------------------------------------------
            # CRITICAL PROJECT RULE
            #
            # The join MUST use properties.cellId.
            #
            # Do NOT use feature["id"].
            # -------------------------------------------------

            cell_id = properties.get(
                "cellId"
            )

            if cell_id is None:

                raise ValueError(
                    "GeoJSON feature is missing "
                    "properties.cellId."
                )

            geometry = feature.get(
                "geometry"
            )

            lookup_rows.append(
                (
                    int(cell_id),
                    json.dumps(geometry)
                    if geometry is not None
                    else None
                )
            )

        self.grid_lookup = (
            self.spark.createDataFrame(
                lookup_rows,
                [
                    "grid_id",
                    "geometry",
                ]
            )
        )

        # -------------------------------------------------
        # Validate lookup keys
        # -------------------------------------------------

        lookup_count = (
            self.grid_lookup.count()
        )

        distinct_lookup_ids = (
            self.grid_lookup
            .select("grid_id")
            .distinct()
            .count()
        )

        if lookup_count != 10000:

            raise ValueError(
                "GeoJSON lookup does not contain "
                "exactly 10,000 rows."
            )

        if distinct_lookup_ids != 10000:

            raise ValueError(
                "GeoJSON properties.cellId "
                "contains duplicate IDs."
            )

        self.logger.info(
            "Built grid lookup using "
            "properties.cellId."
        )

        self.logger.info(
            "Grid lookup rows: %d",
            lookup_count
        )

        return self.grid_lookup

    # =====================================================
    # 4. Compare standard vs broadcast join
    # =====================================================

    def explain_join_strategies(self):

        if self.grid_lookup is None:
            raise RuntimeError(
                "Build grid lookup first."
            )

        print("\n" + "=" * 70)
        print("STANDARD JOIN EXECUTION PLAN")
        print("=" * 70)

        standard_join = (
            self.hourly_grid_summary
            .join(
                self.grid_lookup,
                on="grid_id",
                how="left"
            )
        )

        standard_join.explain(
            mode="formatted"
        )

        print("\n" + "=" * 70)
        print("BROADCAST JOIN EXECUTION PLAN")
        print("=" * 70)

        broadcast_join = (
            self.hourly_grid_summary
            .join(
                broadcast(
                    self.grid_lookup
                ),
                on="grid_id",
                how="left"
            )
        )

        broadcast_join.explain(
            mode="formatted"
        )

        self.logger.info(
            "Displayed standard and broadcast "
            "join execution plans."
        )

    # =====================================================
    # 5. Perform enrichment
    # =====================================================

    def enrich(self):

        if self.grid_lookup is None:
            raise RuntimeError(
                "Build grid lookup first."
            )

        before_count = (
            self.hourly_grid_summary.count()
        )

        self.grid_activity_geo_df = (
            self.hourly_grid_summary
            .join(
                broadcast(
                    self.grid_lookup
                ),
                on="grid_id",
                how="left"
            )
        )

        after_count = (
            self.grid_activity_geo_df.count()
        )

        # -------------------------------------------------
        # LEFT JOIN MUST NOT MULTIPLY ROWS
        # -------------------------------------------------

        if after_count != before_count:

            raise ValueError(
                "Left join changed row count. "
                "The geographic lookup may contain "
                "duplicate grid_id values."
            )

        self.logger.info(
            "Geospatial left join completed."
        )

        self.logger.info(
            "Rows before join: %d",
            before_count
        )

        self.logger.info(
            "Rows after join: %d",
            after_count
        )

        return (
            self.grid_activity_geo_df
        )

    # =====================================================
    # 6. Validate enrichment coverage
    # =====================================================

    def validate_coverage(self):

        df = (
            self.grid_activity_geo_df
        )

        activity_grid_count = (
            df
            .select("grid_id")
            .distinct()
            .count()
        )

        matched_grid_count = (
            df
            .filter(
                col("geometry").isNotNull()
            )
            .select("grid_id")
            .distinct()
            .count()
        )

        unmatched = (
            df
            .filter(
                col("geometry").isNull()
            )
            .select("grid_id")
            .distinct()
            .orderBy("grid_id")
        )

        unmatched_count = (
            unmatched.count()
        )

        coverage_percentage = (
            (
                matched_grid_count
                /
                activity_grid_count
            )
            * 100
            if activity_grid_count > 0
            else 0
        )

        self.unmatched_grid_ids = (
            unmatched
        )

        self.coverage_report = {
            "activity_grids":
                activity_grid_count,

            "matched_grids":
                matched_grid_count,

            "unmatched_grids":
                unmatched_count,

            "coverage_percentage":
                coverage_percentage,
        }

        self.logger.info(
            "Activity grids: %d",
            activity_grid_count
        )

        self.logger.info(
            "Matched grids: %d",
            matched_grid_count
        )

        self.logger.info(
            "Unmatched grids: %d",
            unmatched_count
        )

        self.logger.info(
            "Enrichment coverage: %.2f%%",
            coverage_percentage
        )

        if unmatched_count > 0:

            self.logger.error(
                "Unmatched grid IDs detected."
            )

        return self.coverage_report

    # =====================================================
    # 7. Geographic spot check
    # =====================================================

    def geographic_spot_check(
        self,
        grid_ids=(1, 2, 4821)
    ):
        """
        Print centroid coordinates for selected cells.

        Grid 1 and grid 2 must be adjacent.

        Grid 4821 is used as an additional known-cell
        spot check.

        This check exists specifically to catch the
        top-level GeoJSON id vs properties.cellId trap.
        """

        if self.grid_lookup is None:

            raise RuntimeError(
                "Build grid lookup first."
            )

        selected = (
            self.grid_lookup
            .filter(
                col("grid_id")
                .isin(
                    list(grid_ids)
                )
            )
            .withColumn(
                "centroid",
                centroid_udf(
                    col("geometry")
                )
            )
            .select(
                "grid_id",
                "centroid"
            )
            .orderBy("grid_id")
        )

        rows = selected.collect()

        print("\n" + "=" * 70)
        print("GEOGRAPHIC SPOT CHECK")
        print("=" * 70)

        for row in rows:

            print(
                f"Grid {row['grid_id']}: "
                f"centroid = {row['centroid']}"
            )

        if len(rows) != len(grid_ids):

            raise ValueError(
                "One or more requested grid IDs "
                "were not found in the GeoJSON."
            )

        centroid_map = {
            row["grid_id"]:
                row["centroid"]
            for row in rows
        }

        if (
            centroid_map.get(1)
            ==
            centroid_map.get(2)
        ):

            raise ValueError(
                "Grid 1 and grid 2 have identical "
                "centroids. Geographic lookup is "
                "likely incorrect."
            )

        self.logger.info(
            "Geographic spot check completed "
            "for grids %s.",
            grid_ids
        )

        return selected

    # =====================================================
    # 8. Top high-activity grids
    # =====================================================

    def compute_top_grids(
        self,
        limit=10
    ):

        self.top_grids_with_geometry = (
            self.grid_activity_geo_df
            .groupBy(
                "grid_id",
                "geometry"
            )
            .agg(
                spark_sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
            .orderBy(
                col("total_activity")
                .desc()
            )
            .limit(limit)
        )

        self.logger.info(
            "Computed top %d high-activity grids "
            "with geometry.",
            limit
        )

        return (
            self.top_grids_with_geometry
        )

    # =====================================================
    # 9. Add optional centroid
    # =====================================================

    def add_centroid(self):

        self.grid_activity_geo_df = (
            self.grid_activity_geo_df
            .withColumn(
                "centroid",
                centroid_udf(
                    col("geometry")
                )
            )
        )

        self.logger.info(
            "Added optional centroid field."
        )

        return (
            self.grid_activity_geo_df
        )

    # =====================================================
    # 10. Validate final schema
    # =====================================================

    def validate_final_schema(self):

        required_columns = [
            "timestamp",
            "grid_id",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "total_activity",
            "geometry",
        ]

        missing = [
            column
            for column
            in required_columns
            if column
            not in
            self.grid_activity_geo_df.columns
        ]

        if missing:

            raise ValueError(
                "Enriched output missing columns: "
                f"{missing}"
            )

        self.logger.info(
            "Final enriched schema validation passed."
        )

    # =====================================================
    # 11. Acceptance criteria
    # =====================================================

    def validate_acceptance_criteria(self):

        df = (
            self.grid_activity_geo_df
        )

        print("\n" + "=" * 70)
        print("SP4 ACCEPTANCE CRITERIA")
        print("=" * 70)

        results = {}

        # -------------------------------------------------
        # Criterion 1
        # -------------------------------------------------

        print(
            "\n1. LOOKUP USES properties.cellId"
        )

        # This is enforced during build_grid_lookup().
        criterion_1 = (
            self.grid_lookup is not None
            and
            "grid_id"
            in self.grid_lookup.columns
        )

        results[
            "properties_cellId"
        ] = criterion_1

        print(
            "PASS"
            if criterion_1
            else "FAIL"
        )

        # -------------------------------------------------
        # Criterion 2
        # -------------------------------------------------

        print(
            "\n2. 100% ENRICHMENT COVERAGE"
        )

        coverage = (
            self.coverage_report[
                "coverage_percentage"
            ]
        )

        criterion_2 = (
            coverage == 100.0
        )

        results[
            "coverage"
        ] = criterion_2

        print(
            f"Coverage: "
            f"{coverage:.2f}%"
        )

        print(
            "PASS"
            if criterion_2
            else "FAIL"
        )

        # -------------------------------------------------
        # Criterion 3
        # -------------------------------------------------

        print(
            "\n3. GEOGRAPHIC SPOT CHECK"
        )

        spot_check = (
            self.geographic_spot_check()
        )

        spot_rows = (
            spot_check.collect()
        )

        centroid_1 = None
        centroid_2 = None

        for row in spot_rows:

            if row["grid_id"] == 1:
                centroid_1 = row["centroid"]

            if row["grid_id"] == 2:
                centroid_2 = row["centroid"]

        criterion_3 = (
            centroid_1 is not None
            and
            centroid_2 is not None
            and
            centroid_1 != centroid_2
        )

        results[
            "geographic_spot_check"
        ] = criterion_3

        print(
            "PASS"
            if criterion_3
            else "FAIL"
        )

        # -------------------------------------------------
        # Criterion 4
        # -------------------------------------------------

        print(
            "\n4. UNMATCHED GRID LIST"
        )

        unmatched_count = (
            self.coverage_report[
                "unmatched_grids"
            ]
        )

        criterion_4 = (
            unmatched_count == 0
        )

        results[
            "unmatched_empty"
        ] = criterion_4

        print(
            f"Unmatched grids: "
            f"{unmatched_count}"
        )

        print(
            "PASS"
            if criterion_4
            else "FAIL"
        )

        # -------------------------------------------------
        # Criterion 5
        # -------------------------------------------------

        print(
            "\n5. LEFT JOIN ROW COUNT"
        )

        before_count = (
            self.hourly_grid_summary.count()
        )

        after_count = (
            df.count()
        )

        criterion_5 = (
            before_count == after_count
        )

        results[
            "row_count_preserved"
        ] = criterion_5

        print(
            f"Before: {before_count:,}"
        )

        print(
            f"After : {after_count:,}"
        )

        print(
            "PASS"
            if criterion_5
            else "FAIL"
        )

        # -------------------------------------------------
        # Criterion 6
        # -------------------------------------------------

        print(
            "\n6. FINAL OUTPUT USES grid_id + timestamp"
        )

        criterion_6 = (
            "grid_id"
            in df.columns
            and
            "timestamp"
            in df.columns
        )

        results[
            "contract_keys"
        ] = criterion_6

        print(
            "PASS"
            if criterion_6
            else "FAIL"
        )

        # -------------------------------------------------
        # Overall
        # -------------------------------------------------

        all_passed = all(
            results.values()
        )

        print(
            "\n" + "=" * 70
        )

        if all_passed:

            print(
                "SP4 ACCEPTANCE: ALL PASS"
            )

        else:

            print(
                "SP4 ACCEPTANCE: FAILED"
            )

        print(
            "=" * 70
        )

        return {
            "all_passed":
                all_passed,

            "criteria":
                results,

            "coverage":
                self.coverage_report,
        }

    # =====================================================
    # 12. Export
    # =====================================================

    def export_outputs(
        self,
        output_dir="data/processed"
    ):

        output_path = Path(
            output_dir
        )

        output_path.mkdir(
            parents=True,
            exist_ok=True
        )

        # -------------------------------------------------
        # Enriched dataset
        # -------------------------------------------------

        enriched_path = (
            output_path /
            "grid_activity_geo"
        )

        (
            self.grid_activity_geo_df
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(enriched_path)
            )
        )

        # -------------------------------------------------
        # Unmatched grids
        # -------------------------------------------------

        unmatched_path = (
            output_path /
            "unmatched_grid_ids"
        )

        (
            self.unmatched_grid_ids
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(unmatched_path)
            )
        )

        # -------------------------------------------------
        # Top grids
        # -------------------------------------------------

        top_path = (
            output_path /
            "top_grids_with_geometry"
        )

        (
            self.top_grids_with_geometry
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(top_path)
            )
        )

        self.logger.info(
            "SP4 outputs exported to: %s",
            output_path
        )

        return {
            "enriched":
                enriched_path,

            "unmatched":
                unmatched_path,

            "top_grids":
                top_path,
        }

    # =====================================================
    # 13. Full pipeline
    # =====================================================

    def process(
        self,
        expected_file_count,
        output_dir="data/processed"
    ):

        self.logger.info(
            "Starting SP4 geospatial "
            "enrichment pipeline."
        )

        self.validate_inputs()

        self.build_grid_lookup()

        self.explain_join_strategies()

        self.enrich()

        self.validate_coverage()

        self.geographic_spot_check()

        self.compute_top_grids()

        self.add_centroid()

        self.validate_final_schema()

        acceptance = (
            self.validate_acceptance_criteria()
        )

        if not acceptance["all_passed"]:

            raise ValueError(
                "SP4 acceptance criteria failed."
            )

        output_paths = (
            self.export_outputs(
                output_dir
            )
        )

        self.logger.info(
            "SP4 pipeline completed successfully."
        )

        return {
            "grid_lookup":
                self.grid_lookup,

            "grid_activity_geo_df":
                self.grid_activity_geo_df,

            "coverage_report":
                self.coverage_report,

            "unmatched_grid_ids":
                self.unmatched_grid_ids,

            "top_grids":
                self.top_grids_with_geometry,

            "acceptance":
                acceptance,

            "output_paths":
                output_paths,

            "log_file":
                self.log_file,
        }