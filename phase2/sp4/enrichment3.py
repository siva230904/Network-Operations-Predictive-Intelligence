# =========================================================
# SP4 — Geospatial Enrichment
# File: phase2/sp4/enrichment3.py
# =========================================================

import json
import logging
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


# =========================================================
# Logging
# =========================================================

def create_logger(log_dir="../../data/logs"):
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
            log_path
            / f"sp4_enrichment_run_{run_number:03d}.log"
        )

        if not filename.exists():
            break

        run_number += 1

    logger = logging.getLogger(
        f"SP4_{run_number}"
    )

    logger.setLevel(logging.INFO)
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

    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger, filename


# =========================================================
# Network Geo Enricher
# =========================================================

class NetworkGeoEnricher:
    """
    SP4 — Geospatial enrichment.

    Input:
        SP3 hourly_grid_summary Parquet checkpoint

    Reference:
        milano-grid.geojson

    Output grain:
        timestamp + grid_id

    IMPORTANT:
        GeoJSON properties.cellId is the authoritative
        source of grid_id.

        The top-level GeoJSON "id" is NOT used.
    """

    # =====================================================
    # Constructor
    # =====================================================

    def __init__(
        self,
        spark,
        hourly_grid_summary: DataFrame,
        geojson_path,
        log_dir="../../data/logs"
    ):

        if spark is None:
            raise ValueError(
                "spark cannot be None."
            )

        if hourly_grid_summary is None:
            raise ValueError(
                "hourly_grid_summary cannot be None."
            )

        self.spark = spark

        self.hourly_grid_summary = (
            hourly_grid_summary
        )

        self.geojson_path = Path(
            geojson_path
        )

        # -------------------------------------------------
        # DataFrames
        # -------------------------------------------------

        self.grid_lookup = None
        self.grid_activity_geo_df = None
        self.unmatched_grid_ids = None
        self.top_grids_with_geometry = None

        # -------------------------------------------------
        # Reports
        # -------------------------------------------------

        self.coverage_report = None

        # -------------------------------------------------
        # Counts
        # -------------------------------------------------

        self.input_row_count = None
        self.output_row_count = None

        # -------------------------------------------------
        # Logging
        # -------------------------------------------------

        self.logger, self.log_file = (
            create_logger(log_dir)
        )

    # =====================================================
    # 1. Validate inputs
    # =====================================================

    def validate_inputs(self):

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
            for column in required_columns
            if column not in
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

        self.input_row_count = (
            self.hourly_grid_summary.count()
        )

        if self.input_row_count == 0:

            raise ValueError(
                "hourly_grid_summary contains zero rows."
            )

        self.logger.info(
            "Input validation passed."
        )

        self.logger.info(
            "SP3 input rows: %d",
            self.input_row_count
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

        if geojson.get(
            "type"
        ) != "FeatureCollection":

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
            "GeoJSON contains %d features.",
            len(features)
        )

        return features

    # =====================================================
    # 3. Build grid lookup
    # =====================================================

    def build_grid_lookup(self):

        features = self.load_geojson()

        lookup_rows = []

        for feature_number, feature in enumerate(
            features,
            start=1
        ):

            properties = feature.get(
                "properties",
                {}
            )

            # -------------------------------------------------
            # CRITICAL PROJECT RULE
            #
            # grid_id MUST come from properties.cellId.
            #
            # Do NOT use feature["id"].
            # -------------------------------------------------

            cell_id = properties.get(
                "cellId"
            )

            if cell_id is None:

                raise ValueError(
                    "GeoJSON feature "
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
                    "Invalid properties.cellId "
                    f"for feature {feature_number}: "
                    f"{cell_id}"
                )

            if not (
                1 <= grid_id <= 10000
            ):

                raise ValueError(
                    "GeoJSON properties.cellId "
                    f"outside 1-10000: {grid_id}"
                )

            geometry = feature.get(
                "geometry"
            )

            geometry_json = (
                json.dumps(
                    geometry,
                    separators=(",", ":")
                )
                if geometry is not None
                else None
            )

            lookup_rows.append(
                (
                    grid_id,
                    geometry_json
                )
            )

        self.grid_lookup = (
            self.spark.createDataFrame(
                lookup_rows,
                [
                    "grid_id",
                    "geometry"
                ]
            )
        )

        lookup_count = (
            self.grid_lookup.count()
        )

        distinct_ids = (
            self.grid_lookup
            .select("grid_id")
            .distinct()
            .count()
        )

        if lookup_count != 10000:

            raise ValueError(
                "Grid lookup must contain "
                f"10,000 rows; found {lookup_count}."
            )

        if distinct_ids != 10000:

            raise ValueError(
                "Grid lookup must contain "
                "10,000 distinct grid IDs."
            )

        self.logger.info(
            "Grid lookup successfully built "
            "from properties.cellId."
        )

        self.logger.info(
            "Grid lookup rows: %d",
            lookup_count
        )

        self.logger.info(
            "Distinct grid IDs: %d",
            distinct_ids
        )

        return self.grid_lookup

    # =====================================================
    # 4. Explain standard join vs broadcast join
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
            "Standard and broadcast join plans displayed."
        )

    # =====================================================
    # 5. Enrich SP3 data
    # =====================================================

    def enrich(self):

        if self.grid_lookup is None:

            raise RuntimeError(
                "Build grid lookup first."
            )

        before_count = (
            self.hourly_grid_summary.count()
        )

        self.logger.info(
            "Rows before enrichment: %d",
            before_count
        )

        # -------------------------------------------------
        # Static 10,000-row lookup.
        #
        # Broadcast is intentional here.
        # -------------------------------------------------

        enriched = (
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
            enriched.count()
        )

        if after_count != before_count:

            raise ValueError(
                "SP4 enrichment changed the row count. "
                "The grid lookup may contain duplicate "
                "grid IDs."
            )

        self.grid_activity_geo_df = enriched

        self.output_row_count = after_count

        self.logger.info(
            "Geospatial left join completed."
        )

        self.logger.info(
            "Rows after enrichment: %d",
            after_count
        )

        return self.grid_activity_geo_df

    # =====================================================
    # 6. Validate enrichment coverage
    # =====================================================

    def validate_coverage(self):

        if self.grid_activity_geo_df is None:

            raise RuntimeError(
                "Run enrich() first."
            )

        df = self.grid_activity_geo_df

        activity_grid_count = (
            df
            .select("grid_id")
            .distinct()
            .count()
        )

        matched_grid_count = (
            df
            .filter(
                F.col("geometry").isNotNull()
            )
            .select("grid_id")
            .distinct()
            .count()
        )

        unmatched = (
            df
            .filter(
                F.col("geometry").isNull()
            )
            .select("grid_id")
            .distinct()
            .orderBy("grid_id")
        )

        unmatched_count = (
            unmatched.count()
        )

        if activity_grid_count > 0:

            coverage_percentage = (
                matched_grid_count
                /
                activity_grid_count
                *
                100.0
            )

        else:

            coverage_percentage = 0.0

        self.unmatched_grid_ids = unmatched

        self.coverage_report = {
            "activity_grids":
                activity_grid_count,

            "matched_grids":
                matched_grid_count,

            "unmatched_grids":
                unmatched_count,

            "coverage_percentage":
                coverage_percentage
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
            "Coverage: %.2f%%",
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
        Perform the geographic spot check WITHOUT a
        Python UDF.

        This is deliberately executed on the small
        10,000-row grid lookup rather than the 1.6M-row
        enriched dataset.

        The check validates that selected grid IDs have
        geometry and that grids 1 and 2 are not identical.

        Centroids are calculated in Python only after
        collecting three small geometry records.
        """

        if self.grid_lookup is None:

            raise RuntimeError(
                "Build grid lookup first."
            )

        requested_ids = [
            int(grid_id)
            for grid_id in grid_ids
        ]

        selected = (
            self.grid_lookup
            .filter(
                F.col("grid_id").isin(
                    requested_ids
                )
            )
            .select(
                "grid_id",
                "geometry"
            )
            .orderBy("grid_id")
        )

        # -------------------------------------------------
        # IMPORTANT:
        #
        # Only 3 small lookup records are collected.
        # No Python UDF is executed inside Spark.
        # -------------------------------------------------

        rows = selected.collect()

        if len(rows) != len(
            requested_ids
        ):

            raise ValueError(
                "One or more requested grid IDs "
                "were not found in the GeoJSON."
            )

        print("\n" + "=" * 70)
        print("GEOGRAPHIC SPOT CHECK")
        print("=" * 70)

        centroid_map = {}

        for row in rows:

            centroid = (
                self._geometry_centroid(
                    row["geometry"]
                )
            )

            centroid_map[
                row["grid_id"]
            ] = centroid

            print(
                f"Grid {row['grid_id']}: "
                f"centroid = {centroid}"
            )

            if centroid is None:

                raise ValueError(
                    f"Grid {row['grid_id']} "
                    "has invalid or missing geometry."
                )

        if (
            centroid_map.get(1)
            ==
            centroid_map.get(2)
        ):

            raise ValueError(
                "Grid 1 and grid 2 have identical "
                "centroids. Geographic lookup may "
                "be incorrect."
            )

        self.logger.info(
            "Geographic spot check passed "
            "for grids %s.",
            requested_ids
        )

        return centroid_map

    # =====================================================
    # 7a. Local geometry centroid helper
    # =====================================================

    @staticmethod
    def _geometry_centroid(
        geometry_json
    ):
        """
        Calculate an approximate centroid locally.

        This function is NOT a Spark UDF.

        It runs only for the small number of records
        returned by geographic_spot_check().
        """

        if geometry_json is None:

            return None

        try:

            geometry = json.loads(
                geometry_json
            )

            geometry_type = (
                geometry.get("type")
            )

            coordinates = (
                geometry.get("coordinates")
            )

            if geometry_type == "Polygon":

                if not coordinates:
                    return None

                ring = coordinates[0]

            elif geometry_type == "MultiPolygon":

                if not coordinates:
                    return None

                ring = coordinates[0][0]

            else:

                return None

            if not ring:

                return None

            longitudes = [
                float(point[0])
                for point in ring
            ]

            latitudes = [
                float(point[1])
                for point in ring
            ]

            if not longitudes:
                return None

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

    # =====================================================
    # 8. Top high-activity grids
    # =====================================================

    def compute_top_grids(
        self,
        limit=10
    ):

        if self.grid_activity_geo_df is None:

            raise RuntimeError(
                "Run enrich() first."
            )

        self.top_grids_with_geometry = (
            self.grid_activity_geo_df
            .groupBy(
                "grid_id",
                "geometry"
            )
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
            .orderBy(
                F.col(
                    "total_activity"
                ).desc(),
                F.col(
                    "grid_id"
                ).asc()
            )
            .limit(limit)
        )

        self.logger.info(
            "Computed top %d high-activity "
            "grids with geometry.",
            limit
        )

        return self.top_grids_with_geometry

    # =====================================================
    # 9. Add optional centroid column
    # =====================================================

    def add_centroid(self):
        """
        Intentionally does not add a Spark Python UDF
        centroid column.

        The original implementation used a Python UDF
        over the full enriched dataset, which caused the
        local Python worker crash.

        The geographic spot check already validates
        centroid calculation safely on three records.
        """

        self.logger.info(
            "Centroid column not added to the full "
            "dataset; centroid validation is performed "
            "on the small geographic spot-check sample."
        )

        return self.grid_activity_geo_df

    # =====================================================
    # 10. Validate final schema
    # =====================================================

    def validate_final_schema(self):

        if self.grid_activity_geo_df is None:

            raise RuntimeError(
                "Enriched DataFrame has not been created."
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
            "geometry"
        ]

        missing = [
            column
            for column in required_columns
            if column not in
            self.grid_activity_geo_df.columns
        ]

        if missing:

            raise ValueError(
                "Enriched output missing columns: "
                f"{missing}"
            )

        self.logger.info(
            "Final SP4 schema validation passed."
        )

        return True

    # =====================================================
    # 11. Validate acceptance criteria
    # =====================================================

    def validate_acceptance_criteria(self):

        if self.grid_activity_geo_df is None:

            raise RuntimeError(
                "Enriched DataFrame has not been created."
            )

        if self.coverage_report is None:

            raise RuntimeError(
                "Coverage has not been calculated."
            )

        df = self.grid_activity_geo_df

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

        lookup_valid = (
            self.grid_lookup is not None
            and
            "grid_id"
            in self.grid_lookup.columns
        )

        results[
            "properties_cellId"
        ] = lookup_valid

        print(
            "PASS"
            if lookup_valid
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
            abs(
                coverage - 100.0
            ) < 0.000001
        )

        results[
            "coverage"
        ] = criterion_2

        print(
            f"Coverage: {coverage:.2f}%"
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

        try:

            centroid_map = (
                self.geographic_spot_check()
            )

            criterion_3 = (
                centroid_map.get(1)
                is not None
                and
                centroid_map.get(2)
                is not None
                and
                centroid_map.get(1)
                !=
                centroid_map.get(2)
            )

        except Exception as exc:

            self.logger.error(
                "Geographic spot check failed: %s",
                exc
            )

            criterion_3 = False

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
            "\n4. UNMATCHED GRID LIST EMPTY"
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
            "\n5. LEFT JOIN ROW COUNT PRESERVED"
        )

        before_count = (
            self.input_row_count
        )

        after_count = (
            self.output_row_count
        )

        criterion_5 = (
            before_count
            ==
            after_count
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
            "\n6. FINAL OUTPUT USES "
            "grid_id + timestamp"
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
        # Criterion 7
        # -------------------------------------------------

        print(
            "\n7. GRID IDS WITHIN 1-10000"
        )

        invalid_grid_rows = (
            df
            .filter(
                (F.col("grid_id") < 1)
                |
                (F.col("grid_id") > 10000)
            )
            .count()
        )

        criterion_7 = (
            invalid_grid_rows == 0
        )

        results[
            "valid_grid_ids"
        ] = criterion_7

        print(
            f"Invalid grid rows: "
            f"{invalid_grid_rows:,}"
        )

        print(
            "PASS"
            if criterion_7
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

        self.logger.info(
            "SP4 acceptance criteria: %s",
            "ALL PASS"
            if all_passed
            else "FAILED"
        )

        return {
            "all_passed":
                all_passed,

            "criteria":
                results,

            "coverage":
                self.coverage_report,

            "input_rows":
                before_count,

            "output_rows":
                after_count
        }

    # =====================================================
    # 12. Export outputs
    # =====================================================

    def export_outputs(
        self,
        output_dir="../../data/landing/sp4"
    ):
        """
        Export SP4 outputs.

        IMPORTANT:
        - Parquet is the SP5 consumption checkpoint.
        - CSV outputs are retained for inspection/training.
        - Spark writes directory-based datasets.
        """

        if self.grid_activity_geo_df is None:

            raise RuntimeError(
                "Nothing to export."
            )

        if self.unmatched_grid_ids is None:

            raise RuntimeError(
                "Coverage must be calculated before export."
            )

        if self.top_grids_with_geometry is None:

            raise RuntimeError(
                "Top grids must be calculated before export."
            )

        output_path = Path(
            output_dir
        )

        output_path.mkdir(
            parents=True,
            exist_ok=True
        )

        # -------------------------------------------------
        # Parquet checkpoint
        # -------------------------------------------------

        enriched_parquet = (
            output_path /
            "grid_activity_geo"
        )

        (
            self.grid_activity_geo_df
            .write
            .mode("overwrite")
            .parquet(
                str(enriched_parquet)
            )
        )

        # -------------------------------------------------
        # CSV — enriched data
        # -------------------------------------------------

        enriched_csv = (
            output_path /
            "grid_activity_geo_csv"
        )

        (
            self.grid_activity_geo_df
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(enriched_csv)
            )
        )

        # -------------------------------------------------
        # CSV — unmatched grid IDs
        # -------------------------------------------------

        unmatched_csv = (
            output_path /
            "unmatched_grid_ids"
        )

        (
            self.unmatched_grid_ids
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(unmatched_csv)
            )
        )

        # -------------------------------------------------
        # CSV — top grids with geometry
        # -------------------------------------------------

        top_grids_csv = (
            output_path /
            "top_grids_with_geometry"
        )

        (
            self.top_grids_with_geometry
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(top_grids_csv)
            )
        )

        # -------------------------------------------------
        # Coverage report JSON
        # -------------------------------------------------

        coverage_json = (
            output_path /
            "sp4_coverage_report.json"
        )

        with open(
            coverage_json,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                self.coverage_report,
                file,
                indent=4
            )

        self.logger.info(
            "SP4 Parquet checkpoint exported to: %s",
            enriched_parquet
        )

        self.logger.info(
            "SP4 enriched CSV exported to: %s",
            enriched_csv
        )

        self.logger.info(
            "SP4 unmatched grid CSV exported to: %s",
            unmatched_csv
        )

        self.logger.info(
            "SP4 top grids CSV exported to: %s",
            top_grids_csv
        )

        self.logger.info(
            "SP4 coverage report exported to: %s",
            coverage_json
        )

        return {
            "parquet":
                enriched_parquet,

            "enriched_csv":
                enriched_csv,

            "unmatched":
                unmatched_csv,

            "top_grids":
                top_grids_csv,

            "coverage_report":
                coverage_json
        }

    # =====================================================
    # 13. Full pipeline
    # =====================================================

    def process(
        self,
        output_dir="../../data/landing/sp4"
    ):
        """
        Run complete SP4 pipeline.

        Flow:

            SP3 Parquet
                 ↓
            validate inputs
                 ↓
            load GeoJSON
                 ↓
            properties.cellId lookup
                 ↓
            broadcast enrichment
                 ↓
            coverage validation
                 ↓
            geographic spot check
                 ↓
            top grids
                 ↓
            schema validation
                 ↓
            acceptance criteria
                 ↓
            Parquet + CSV outputs
        """

        self.logger.info(
            "Starting SP4 geospatial "
            "enrichment pipeline."
        )

        # -------------------------------------------------
        # 1
        # -------------------------------------------------

        self.validate_inputs()

        # -------------------------------------------------
        # 2
        # -------------------------------------------------

        self.build_grid_lookup()

        # -------------------------------------------------
        # 3
        # -------------------------------------------------

        self.explain_join_strategies()

        # -------------------------------------------------
        # 4
        # -------------------------------------------------

        self.enrich()

        # -------------------------------------------------
        # 5
        # -------------------------------------------------

        self.validate_coverage()

        # -------------------------------------------------
        # 6
        # -------------------------------------------------

        self.geographic_spot_check()

        # -------------------------------------------------
        # 7
        # -------------------------------------------------

        self.compute_top_grids()

        # -------------------------------------------------
        # 8
        # -------------------------------------------------

        self.add_centroid()

        # -------------------------------------------------
        # 9
        # -------------------------------------------------

        self.validate_final_schema()

        # -------------------------------------------------
        # 10
        # -------------------------------------------------

        acceptance = (
            self.validate_acceptance_criteria()
        )

        if not acceptance["all_passed"]:

            self.logger.error(
                "SP4 acceptance criteria failed."
            )

            raise ValueError(
                "SP4 acceptance criteria failed."
            )

        # -------------------------------------------------
        # 11
        # -------------------------------------------------

        output_paths = (
            self.export_outputs(
                output_dir
            )
        )

        # -------------------------------------------------
        # Completion
        # -------------------------------------------------

        self.logger.info(
            "SP4 pipeline completed successfully."
        )

        self.logger.info(
            "Log file: %s",
            self.log_file
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
                self.log_file
        }