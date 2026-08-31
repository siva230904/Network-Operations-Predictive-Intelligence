# =========================================================
# SP4 — Geospatial Enrichment
# File: phase2/sp4/enrichment2.py
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
# Geometry helper
# =========================================================

def calculate_centroid(geometry):
    """
    Calculate an approximate centroid for a GeoJSON
    Polygon or MultiPolygon.

    Coordinates are expected as:

        [longitude, latitude]

    This calculation is performed only for the
    10,000-cell reference lookup, NOT for the millions
    of activity rows.
    """

    if geometry is None:
        return None

    try:

        geometry_type = geometry.get(
            "type"
        )

        coordinates = geometry.get(
            "coordinates"
        )

        if geometry_type == "Polygon":

            if not coordinates:
                return None

            ring = coordinates[0]

        elif geometry_type == "MultiPolygon":

            if not coordinates:
                return None

            if not coordinates[0]:
                return None

            ring = coordinates[0][0]

        else:

            return None

        if not ring:
            return None

        longitude_sum = 0.0
        latitude_sum = 0.0
        point_count = 0

        for point in ring:

            if len(point) < 2:
                continue

            longitude_sum += float(
                point[0]
            )

            latitude_sum += float(
                point[1]
            )

            point_count += 1

        if point_count == 0:
            return None

        centroid_longitude = (
            longitude_sum
            /
            point_count
        )

        centroid_latitude = (
            latitude_sum
            /
            point_count
        )

        return (
            f"{centroid_longitude:.6f},"
            f"{centroid_latitude:.6f}"
        )

    except Exception:
        return None


# =========================================================
# SP4 Geospatial Enricher
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

    CRITICAL:

        grid_id MUST come from:

            properties.cellId

        Never use the top-level GeoJSON "id".
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

        self.grid_lookup = None

        self.grid_activity_geo_df = None

        self.unmatched_grid_ids = None

        self.coverage_report = None

        self.top_grids_with_geometry = None

        self.standard_join = None

        self.broadcast_join = None

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
            if column
            not in self.hourly_grid_summary.columns
        ]

        if missing:

            self.logger.error(
                "Missing required activity columns: %s",
                missing
            )

            raise ValueError(
                "hourly_grid_summary is missing: "
                f"{missing}"
            )

        if not self.geojson_path.exists():

            self.logger.error(
                "GeoJSON not found: %s",
                self.geojson_path
            )

            raise FileNotFoundError(
                f"GeoJSON not found: "
                f"{self.geojson_path}"
            )

        if (
            not self.geojson_path.is_file()
        ):

            raise ValueError(
                "geojson_path is not a file: "
                f"{self.geojson_path}"
            )

        self.logger.info(
            "Input validation passed."
        )

    # =====================================================
    # 2. Inspect GeoJSON
    # =====================================================

    def inspect_geojson(self):

        with open(
            self.geojson_path,
            "r",
            encoding="utf-8"
        ) as file:

            geojson = json.load(file)

        # -------------------------------------------------
        # Top-level structure
        # -------------------------------------------------

        geojson_type = geojson.get(
            "type"
        )

        if geojson_type != "FeatureCollection":

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

        if not features:

            raise ValueError(
                "GeoJSON contains no features."
            )

        # -------------------------------------------------
        # Inspect first feature
        # -------------------------------------------------

        first_feature = features[0]

        top_level_id = (
            first_feature.get("id")
        )

        properties = (
            first_feature.get(
                "properties",
                {}
            )
        )

        property_keys = list(
            properties.keys()
        )

        geometry = (
            first_feature.get(
                "geometry"
            )
        )

        geometry_type = (
            geometry.get("type")
            if geometry
            else None
        )

        self.logger.info(
            "GeoJSON type: %s",
            geojson_type
        )

        self.logger.info(
            "Feature count: %d",
            len(features)
        )

        self.logger.info(
            "Example top-level feature id: %s",
            top_level_id
        )

        self.logger.info(
            "Example feature property keys: %s",
            property_keys
        )

        self.logger.info(
            "Example geometry type: %s",
            geometry_type
        )

        print("\n" + "=" * 70)
        print("GEOJSON STRUCTURE")
        print("=" * 70)

        print(
            f"Top-level type : {geojson_type}"
        )

        print(
            f"Feature count  : {len(features):,}"
        )

        print(
            f"Top-level id   : {top_level_id}"
        )

        print(
            f"Properties     : {property_keys}"
        )

        print(
            f"Geometry type  : {geometry_type}"
        )

        print(
            "Join key       : properties.cellId"
        )

        print(
            "NOT join key   : top-level feature id"
        )

        print(
            "=" * 70
        )

        return {
            "type":
                geojson_type,

            "feature_count":
                len(features),

            "example_top_level_id":
                top_level_id,

            "property_keys":
                property_keys,

            "geometry_type":
                geometry_type,
        }

    # =====================================================
    # 3. Load GeoJSON
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
            "Loaded %d GeoJSON features.",
            len(features)
        )

        return features

    # =====================================================
    # 4. Build grid lookup
    # =====================================================

    def build_grid_lookup(self):
        """
        Flatten GeoJSON into:

            grid_id
            geometry
            centroid

        CRITICAL PROJECT RULE:

            grid_id =
            properties.cellId

        Never feature["id"].
        """

        features = self.load_geojson()

        lookup_rows = []

        for feature in features:

            properties = feature.get(
                "properties",
                {}
            )

            # -------------------------------------------------
            # CRITICAL:
            #
            # properties.cellId is 1-based and is the
            # canonical grid identifier.
            #
            # The top-level feature["id"] is 0-based and
            # MUST NOT be used for this join.
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

            if geometry is None:

                raise ValueError(
                    f"Grid {cell_id} has no geometry."
                )

            geometry_json = json.dumps(
                geometry,
                separators=(",", ":")
            )

            centroid = (
                calculate_centroid(
                    geometry
                )
            )

            lookup_rows.append(
                (
                    int(cell_id),
                    geometry_json,
                    centroid,
                )
            )

        self.grid_lookup = (
            self.spark.createDataFrame(
                lookup_rows,
                [
                    "grid_id",
                    "geometry",
                    "centroid",
                ]
            )
        )

        # -------------------------------------------------
        # Validate row count
        # -------------------------------------------------

        lookup_count = (
            self.grid_lookup.count()
        )

        if lookup_count != 10000:

            raise ValueError(
                "GeoJSON lookup does not contain "
                "exactly 10,000 rows."
            )

        # -------------------------------------------------
        # Validate distinct IDs
        # -------------------------------------------------

        distinct_ids = (
            self.grid_lookup
            .select("grid_id")
            .distinct()
            .count()
        )

        if distinct_ids != 10000:

            raise ValueError(
                "GeoJSON properties.cellId "
                "contains duplicate IDs."
            )

        # -------------------------------------------------
        # Validate ID range
        # -------------------------------------------------

        invalid_ids = (
            self.grid_lookup
            .filter(
                (F.col("grid_id") < 1)
                |
                (F.col("grid_id") > 10000)
            )
            .count()
        )

        if invalid_ids > 0:

            raise ValueError(
                "GeoJSON contains grid IDs "
                "outside 1-10000."
            )

        self.logger.info(
            "Grid lookup built from "
            "properties.cellId."
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
    # 5. Compare standard vs broadcast join
    # =====================================================

    def explain_join_strategies(self):
        """
        Compare the physical plans for:

            standard join
            broadcast join

        The grid lookup is an excellent broadcast candidate
        because it contains only 10,000 rows.
        """

        if self.grid_lookup is None:

            raise RuntimeError(
                "Build grid lookup first."
            )

        self.standard_join = (
            self.hourly_grid_summary
            .join(
                self.grid_lookup,
                on="grid_id",
                how="left"
            )
        )

        self.broadcast_join = (
            self.hourly_grid_summary
            .join(
                broadcast(
                    self.grid_lookup
                ),
                on="grid_id",
                how="left"
            )
        )

        print("\n" + "=" * 70)
        print("STANDARD JOIN EXECUTION PLAN")
        print("=" * 70)

        self.standard_join.explain(
            mode="formatted"
        )

        print("\n" + "=" * 70)
        print("BROADCAST JOIN EXECUTION PLAN")
        print("=" * 70)

        self.broadcast_join.explain(
            mode="formatted"
        )

        print("\n" + "=" * 70)
        print("JOIN STRATEGY")
        print("=" * 70)

        print(
            "The Milan grid lookup contains only "
            "10,000 cells."
        )

        print(
            "The activity dataset contains many more "
            "grid/hour records."
        )

        print(
            "Therefore the grid lookup is an appropriate "
            "broadcast candidate."
        )

        print(
            "=" * 70
        )

        self.logger.info(
            "Displayed standard and broadcast "
            "join execution plans."
        )

    # =====================================================
    # 6. Perform enrichment
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
            "Rows before geospatial join: %d",
            before_count
        )

        # -------------------------------------------------
        # Broadcast the 10,000-row static reference.
        # -------------------------------------------------

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

            self.logger.error(
                "Row count changed during left join. "
                "Before=%d After=%d",
                before_count,
                after_count
            )

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

        return self.grid_activity_geo_df

    # =====================================================
    # 7. Select final output columns
    # =====================================================

    def select_final_columns(self):

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
            "centroid",
        ]

        missing = [
            column
            for column in required_columns
            if column
            not in self.grid_activity_geo_df.columns
        ]

        if missing:

            raise ValueError(
                "Final enriched dataset is missing: "
                f"{missing}"
            )

        self.grid_activity_geo_df = (
            self.grid_activity_geo_df
            .select(
                *required_columns
            )
        )

        self.logger.info(
            "Final enriched columns selected."
        )

        return self.grid_activity_geo_df

    # =====================================================
    # 8. Validate enrichment coverage
    # =====================================================

    def validate_coverage(self):

        if self.grid_activity_geo_df is None:

            raise RuntimeError(
                "Run enrich() first."
            )

        df = self.grid_activity_geo_df

        # -------------------------------------------------
        # Distinct activity grids
        # -------------------------------------------------

        activity_grid_count = (
            df
            .select("grid_id")
            .distinct()
            .count()
        )

        # -------------------------------------------------
        # Matched grids
        # -------------------------------------------------

        matched_grid_count = (
            df
            .filter(
                F.col("geometry").isNotNull()
            )
            .select("grid_id")
            .distinct()
            .count()
        )

        # -------------------------------------------------
        # Unmatched grids
        # -------------------------------------------------

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

        # -------------------------------------------------
        # Coverage
        # -------------------------------------------------

        if activity_grid_count > 0:

            coverage_percentage = (
                matched_grid_count
                /
                activity_grid_count
                *
                100
            )

        else:

            coverage_percentage = 0.0

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
    # 9. Geographic spot check
    # =====================================================

    def geographic_spot_check(
        self,
        grid_ids=(1, 2, 4821)
    ):
        """
        Print centroid coordinates for selected cells.

        Grid 1 and grid 2 must be adjacent.

        This specifically helps detect the dangerous
        top-level id vs properties.cellId mismatch.
        """

        if self.grid_lookup is None:

            raise RuntimeError(
                "Build grid lookup first."
            )

        selected = (
            self.grid_lookup
            .filter(
                F.col("grid_id")
                .isin(
                    list(grid_ids)
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

        # -------------------------------------------------
        # Grid 1 and grid 2 cannot have the same centroid.
        # -------------------------------------------------

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
    # 10. Top high-activity grids
    # =====================================================

    def compute_top_grids(
        self,
        limit=10
    ):
        """
        Identify top high-activity grids and retain geometry.
        """

        if self.grid_activity_geo_df is None:

            raise RuntimeError(
                "Run enrich() first."
            )

        self.top_grids_with_geometry = (
            self.grid_activity_geo_df
            .groupBy(
                "grid_id",
                "geometry",
                "centroid"
            )
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
            .orderBy(
                F.desc(
                    "total_activity"
                )
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
    # 11. Validate final schema
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
            not in self.grid_activity_geo_df.columns
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
    # 12. Validate acceptance criteria
    # =====================================================

    def validate_acceptance_criteria(self):

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

                centroid_1 = (
                    row["centroid"]
                )

            if row["grid_id"] == 2:

                centroid_2 = (
                    row["centroid"]
                )

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
            f"Before: "
            f"{before_count:,}"
        )

        print(
            f"After : "
            f"{after_count:,}"
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
        # Criterion 7
        # -------------------------------------------------

        print(
            "\n7. COUNTRY_CODE NOT IN ENRICHED OUTPUT"
        )

        criterion_7 = (
            "country_code"
            not in df.columns
        )

        results[
            "country_code_removed"
        ] = criterion_7

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
        }

    # =====================================================
    # 13. Export outputs
    # =====================================================

    def export_outputs(
        self,
        output_dir="../../data/landing/sp4"
    ):
        """
        Persist SP4 outputs.

        Spark CSV output is stored as directories containing
        part files, consistent with the existing SP2/SP3
        checkpoint approach.
        """

        output_path = Path(
            output_dir
        )

        output_path.mkdir(
            parents=True,
            exist_ok=True
        )

        # -------------------------------------------------
        # Enriched activity
        # -------------------------------------------------

        enriched_path = (
            output_path
            /
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
        # Unmatched grid IDs
        # -------------------------------------------------

        unmatched_path = (
            output_path
            /
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
            output_path
            /
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

        # -------------------------------------------------
        # Coverage report
        # -------------------------------------------------

        coverage_path = (
            output_path
            /
            "grid_enrichment_coverage.json"
        )

        with open(
            coverage_path,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                self.coverage_report,
                file,
                indent=4
            )

        self.logger.info(
            "Enriched dataset exported to: %s",
            enriched_path
        )

        self.logger.info(
            "Unmatched grid list exported to: %s",
            unmatched_path
        )

        self.logger.info(
            "Top grids exported to: %s",
            top_path
        )

        self.logger.info(
            "Coverage report exported to: %s",
            coverage_path
        )

        return {
            "enriched":
                enriched_path,

            "unmatched":
                unmatched_path,

            "top_grids":
                top_path,

            "coverage":
                coverage_path,
        }

    # =====================================================
    # 14. Full pipeline
    # =====================================================

    def process(
        self,
        output_dir="../../data/landing/sp4"
    ):
        """
        Run the complete SP4 pipeline.

        Flow:

            SP3 hourly checkpoint
                    ↓
            GeoJSON inspection
                    ↓
            properties.cellId lookup
                    ↓
            broadcast left join
                    ↓
            coverage validation
                    ↓
            geographic spot check
                    ↓
            hotspot geometry
                    ↓
            acceptance tests
                    ↓
            persisted SP4 outputs
        """

        self.logger.info(
            "Starting SP4 geospatial "
            "enrichment pipeline."
        )

        # -------------------------------------------------
        # Validation
        # -------------------------------------------------

        self.validate_inputs()

        # -------------------------------------------------
        # Inspect reference
        # -------------------------------------------------

        self.inspect_geojson()

        # -------------------------------------------------
        # Build static lookup
        # -------------------------------------------------

        self.build_grid_lookup()

        # -------------------------------------------------
        # Compare join strategies
        # -------------------------------------------------

        self.explain_join_strategies()

        # -------------------------------------------------
        # Enrich
        # -------------------------------------------------

        self.enrich()

        # -------------------------------------------------
        # Keep contract fields
        # -------------------------------------------------

        self.select_final_columns()

        # -------------------------------------------------
        # Validate coverage
        # -------------------------------------------------

        self.validate_coverage()

        # -------------------------------------------------
        # Geographic validation
        # -------------------------------------------------

        self.geographic_spot_check()

        # -------------------------------------------------
        # Hotspots
        # -------------------------------------------------

        self.compute_top_grids()

        # -------------------------------------------------
        # Schema validation
        # -------------------------------------------------

        self.validate_final_schema()

        # -------------------------------------------------
        # Acceptance criteria
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
        # Export
        # -------------------------------------------------

        output_paths = (
            self.export_outputs(
                output_dir
            )
        )

        # -------------------------------------------------
        # Complete
        # -------------------------------------------------

        self.logger.info(
            "SP4 geospatial enrichment "
            "pipeline completed successfully."
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
                self.log_file,
        }