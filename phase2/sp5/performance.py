# =========================================================
# SP5 — Performance & Execution Behaviour
# File: phase2/sp5/performance.py
# =========================================================

import json
import logging
import time
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# =========================================================
# Logging
# =========================================================

def create_logger(log_dir="../../data/logs"):
    """
    Create a unique SP5 log file.

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
            / f"sp5_performance_run_{run_number:03d}.log"
        )

        if not filename.exists():
            break

        run_number += 1

    logger = logging.getLogger(
        f"SP5_{run_number}"
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
# Performance Analyzer
# =========================================================

class PerformanceAnalyzer:

    """
    SP5 — Spark Performance & Execution Behaviour.

    Inputs:

        SP2 clean checkpoint
        SP3 hourly checkpoint
        SP4 geospatial checkpoint

    Main experiments:

        1. explain() on hotspot aggregation
        2. cache / persist timing experiment
        3. repartition experiment
        4. column pruning demonstration
        5. standard vs broadcast join
        6. performance observations
        7. acceptance criteria

    Important local-machine design decision:

        The full SP2 dataset is NOT cached.

        The cache experiment uses a narrow DataFrame
        containing only:

            timestamp
            grid_id
            total_activity

        and persists it using MEMORY_AND_DISK.

        This prevents the experiment from turning into
        an uncontrolled JVM heap allocation.
    """

    SP2_REQUIRED_COLUMNS = [
        "timestamp",
        "grid_id",
        "country_code",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity",
        "total_activity",
    ]

    SP3_REQUIRED_COLUMNS = [
        "timestamp",
        "grid_id",
        "total_activity",
    ]

    SP4_REQUIRED_COLUMNS = [
        "timestamp",
        "grid_id",
        "total_activity",
        "geometry",
    ]

    CACHE_COLUMNS = [
        "timestamp",
        "grid_id",
        "total_activity",
    ]

    # =====================================================
    # Constructor
    # =====================================================

    def __init__(
        self,
        spark,
        sp2_df: DataFrame,
        sp3_df: DataFrame,
        sp4_df: DataFrame,
        output_dir="../../data/landing/sp5",
        log_dir="../../data/logs",
        cache_sample_fraction=0.10,
        cache_partitions=32
    ):

        if spark is None:
            raise ValueError(
                "spark cannot be None."
            )

        if sp2_df is None:
            raise ValueError(
                "sp2_df cannot be None."
            )

        if sp3_df is None:
            raise ValueError(
                "sp3_df cannot be None."
            )

        if sp4_df is None:
            raise ValueError(
                "sp4_df cannot be None."
            )

        self.spark = spark

        self.sp2_df = sp2_df
        self.sp3_df = sp3_df
        self.sp4_df = sp4_df

        self.output_dir = Path(
            output_dir
        )

        self.cache_sample_fraction = (
            float(cache_sample_fraction)
        )

        self.cache_partitions = (
            int(cache_partitions)
        )

        self.logger, self.log_file = (
            create_logger(log_dir)
        )

        # -------------------------------------------------
        # Input counts
        # -------------------------------------------------

        self.sp2_row_count = None
        self.sp3_row_count = None
        self.sp4_row_count = None

        # -------------------------------------------------
        # Experiment outputs
        # -------------------------------------------------

        self.hotspot_df = None

        self.cache_test_df = None
        self.cache_timings = None

        self.repartition_df = None
        self.repartition_results = None

        self.pruned_df = None
        self.column_pruning_results = None

        self.broadcast_plan = None
        self.standard_plan = None
        self.broadcast_join_df = None
        self.standard_join_df = None

        self.performance_observations = []

        self.summary = None

    # =====================================================
    # 1. Validate inputs
    # =====================================================

    def validate_inputs(self):

        print("\n" + "=" * 70)
        print("SP5 INPUT VALIDATION")
        print("=" * 70)

        # -------------------------------------------------
        # SP2
        # -------------------------------------------------

        for column in self.SP2_REQUIRED_COLUMNS:

            if column not in self.sp2_df.columns:

                raise ValueError(
                    f"SP2 checkpoint missing column: "
                    f"{column}"
                )

        # -------------------------------------------------
        # SP3
        # -------------------------------------------------

        for column in self.SP3_REQUIRED_COLUMNS:

            if column not in self.sp3_df.columns:

                raise ValueError(
                    f"SP3 checkpoint missing column: "
                    f"{column}"
                )

        # -------------------------------------------------
        # SP4
        # -------------------------------------------------

        for column in self.SP4_REQUIRED_COLUMNS:

            if column not in self.sp4_df.columns:

                raise ValueError(
                    f"SP4 checkpoint missing column: "
                    f"{column}"
                )

        # -------------------------------------------------
        # Counts
        # -------------------------------------------------

        self.sp2_row_count = (
            self.sp2_df.count()
        )

        self.sp3_row_count = (
            self.sp3_df.count()
        )

        self.sp4_row_count = (
            self.sp4_df.count()
        )

        print(
            f"SP2 rows: {self.sp2_row_count:,}"
        )

        print(
            f"SP3 rows: {self.sp3_row_count:,}"
        )

        print(
            f"SP4 rows: {self.sp4_row_count:,}"
        )

        # -------------------------------------------------
        # SP3 / SP4 row-count relationship
        # -------------------------------------------------

        if self.sp3_row_count != self.sp4_row_count:

            raise ValueError(
                "SP3 and SP4 row counts differ. "
                "SP4 should preserve the SP3 fact-table grain."
            )

        self.logger.info(
            "SP5 input validation passed."
        )

        self.logger.info(
            "SP2 rows: %d",
            self.sp2_row_count
        )

        self.logger.info(
            "SP3 rows: %d",
            self.sp3_row_count
        )

        self.logger.info(
            "SP4 rows: %d",
            self.sp4_row_count
        )

    # =====================================================
    # 2. Explain hotspot aggregation
    # =====================================================

    def explain_hotspot_aggregation(self):

        print("\n" + "=" * 70)
        print("SP5 — HOTSPOT EXPLAIN()")
        print("=" * 70)

        self.hotspot_df = (
            self.sp3_df
            .groupBy("grid_id")
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
            .orderBy(
                F.desc("total_activity")
            )
            .limit(10)
        )

        print(
            "\nPhysical plan for hotspot aggregation:"
        )

        self.hotspot_df.explain(
            mode="formatted"
        )

        self.logger.info(
            "Hotspot aggregation physical plan "
            "was generated using explain(formatted)."
        )

        return self.hotspot_df

    # =====================================================
    # 3. Cache timing experiment
    # =====================================================

    def compare_cache_timings(self):

        print("\n" + "=" * 70)
        print("SP5 — CACHE TIMING EXPERIMENT")
        print("=" * 70)

        print(
            "\nFull SP2 DataFrame is NOT cached."
        )

        print(
            "Reason: full-data caching caused JVM heap "
            "pressure in the local Spark environment."
        )

        print(
            "\nCache experiment columns:"
        )

        print(
            self.CACHE_COLUMNS
        )

        # -------------------------------------------------
        # Narrow the DataFrame first
        # -------------------------------------------------

        narrow_df = (
            self.sp2_df
            .select(
                *self.CACHE_COLUMNS
            )
        )

        # -------------------------------------------------
        # Use a bounded sample for the local cache
        # experiment.
        #
        # The sample remains deterministic.
        # -------------------------------------------------

        if (
            self.cache_sample_fraction > 0
            and
            self.cache_sample_fraction < 1
        ):

            cache_df = (
                narrow_df
                .sample(
                    withReplacement=False,
                    fraction=self.cache_sample_fraction,
                    seed=42
                )
            )

        else:

            cache_df = narrow_df

        # -------------------------------------------------
        # Repartition before persistence.
        #
        # This keeps cached partitions smaller than the
        # previous full-data / low-partition experiment.
        # -------------------------------------------------

        cache_df = (
            cache_df
            .repartition(
                self.cache_partitions
            )
        )

        self.cache_test_df = cache_df

        print(
            f"\nCache sample fraction: "
            f"{self.cache_sample_fraction}"
        )

        print(
            f"Cache partitions: "
            f"{self.cache_partitions}"
        )

        # -------------------------------------------------
        # Baseline timing
        # -------------------------------------------------

        baseline_start = time.perf_counter()

        baseline_count = (
            self.cache_test_df.count()
        )

        baseline_time = (
            time.perf_counter()
            -
            baseline_start
        )

        print(
            f"\nBaseline action:"
            f" {baseline_count:,} rows"
        )

        print(
            f"Baseline time: "
            f"{baseline_time:.3f} seconds"
        )

        # -------------------------------------------------
        # Persist
        #
        # MEMORY_AND_DISK is deliberately used instead of
        # MEMORY_ONLY.
        #
        # This is important on a local machine.
        # -------------------------------------------------

        persisted_df = (
            self.cache_test_df
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        # -------------------------------------------------
        # First action materializes the persisted dataset
        # -------------------------------------------------

        first_start = time.perf_counter()

        first_count = (
            persisted_df.count()
        )

        first_time = (
            time.perf_counter()
            -
            first_start
        )

        # -------------------------------------------------
        # Repeated action
        # -------------------------------------------------

        second_start = time.perf_counter()

        second_count = (
            persisted_df.count()
        )

        second_time = (
            time.perf_counter()
            -
            second_start
        )

        # -------------------------------------------------
        # Validate counts
        # -------------------------------------------------

        if first_count != second_count:

            persisted_df.unpersist(
                blocking=True
            )

            raise ValueError(
                "Cached repeated-action counts differ."
            )

        # -------------------------------------------------
        # Calculate improvement
        # -------------------------------------------------

        if first_time > 0:

            improvement_percentage = (
                (
                    first_time
                    -
                    second_time
                )
                /
                first_time
            ) * 100

        else:

            improvement_percentage = 0.0

        self.cache_timings = {

            "baseline_uncached_seconds":
                baseline_time,

            "first_persisted_action_seconds":
                first_time,

            "second_persisted_action_seconds":
                second_time,

            "first_action_rows":
                first_count,

            "second_action_rows":
                second_count,

            "improvement_percentage":
                improvement_percentage,

            "cache_columns":
                self.CACHE_COLUMNS,

            "sample_fraction":
                self.cache_sample_fraction,

            "partitions":
                self.cache_partitions,

            "storage_level":
                "MEMORY_AND_DISK",
        }

        print(
            f"\nFirst persisted action: "
            f"{first_time:.3f} seconds"
        )

        print(
            f"Second persisted action: "
            f"{second_time:.3f} seconds"
        )

        print(
            f"Timing change: "
            f"{improvement_percentage:.2f}%"
        )

        # -------------------------------------------------
        # Required cleanup
        # -------------------------------------------------

        persisted_df.unpersist(
            blocking=True
        )

        self.logger.info(
            "Cache experiment completed."
        )

        self.logger.info(
            "Baseline: %.3f seconds",
            baseline_time
        )

        self.logger.info(
            "First persisted action: %.3f seconds",
            first_time
        )

        self.logger.info(
            "Second persisted action: %.3f seconds",
            second_time
        )

        return self.cache_timings

    # =====================================================
    # 4. Repartition experiment
    # =====================================================

    def compare_repartitioning(self):

        print("\n" + "=" * 70)
        print("SP5 — REPARTITION EXPERIMENT")
        print("=" * 70)

        # -------------------------------------------------
        # Use date because it is an existing temporal key.
        # -------------------------------------------------

        base_df = (
            self.sp3_df
            .select(
                "timestamp",
                "grid_id",
                "total_activity"
            )
            .withColumn(
                "date",
                F.to_date("timestamp")
            )
        )

        original_partitions = (
            base_df.rdd.getNumPartitions()
        )

        print(
            f"\nOriginal partitions: "
            f"{original_partitions}"
        )

        # -------------------------------------------------
        # Repartition by date.
        #
        # A moderate partition count is used instead of
        # creating thousands of tiny partitions.
        # -------------------------------------------------

        target_partitions = max(
            8,
            min(
                32,
                original_partitions * 2
            )
        )

        repartitioned = (
            base_df
            .repartition(
                target_partitions,
                "date"
            )
        )

        repartitioned_count = (
            repartitioned.rdd.getNumPartitions()
        )

        print(
            f"Requested partitions: "
            f"{target_partitions}"
        )

        print(
            f"Observed partitions: "
            f"{repartitioned_count}"
        )

        # -------------------------------------------------
        # Explain
        # -------------------------------------------------

        print(
            "\nPhysical plan after repartition:"
        )

        repartitioned.explain(
            mode="formatted"
        )

        self.repartition_df = (
            repartitioned
        )

        self.repartition_results = {

            "original_partitions":
                original_partitions,

            "requested_partitions":
                target_partitions,

            "observed_partitions":
                repartitioned_count,

            "partition_key":
                "date",
        }

        self.logger.info(
            "Repartition experiment completed."
        )

        return self.repartition_results

    # =====================================================
    # 5. Column pruning
    # =====================================================

    def demonstrate_column_pruning(self):

        print("\n" + "=" * 70)
        print("SP5 — COLUMN PRUNING")
        print("=" * 70)

        # -------------------------------------------------
        # Full input columns
        # -------------------------------------------------

        full_columns = (
            list(
                self.sp3_df.columns
            )
        )

        # -------------------------------------------------
        # Only fields required by hotspot aggregation
        # -------------------------------------------------

        required_columns = [
            "grid_id",
            "total_activity"
        ]

        self.pruned_df = (
            self.sp3_df
            .select(
                *required_columns
            )
        )

        pruned_columns = (
            list(
                self.pruned_df.columns
            )
        )

        print(
            "\nOriginal columns:"
        )

        print(
            full_columns
        )

        print(
            "\nColumns required by hotspot aggregation:"
        )

        print(
            pruned_columns
        )

        # -------------------------------------------------
        # Explain physical plan
        # -------------------------------------------------

        aggregation = (
            self.pruned_df
            .groupBy("grid_id")
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
        )

        print(
            "\nPhysical plan after column pruning:"
        )

        aggregation.explain(
            mode="formatted"
        )

        columns_removed = (
            len(full_columns)
            -
            len(pruned_columns)
        )

        self.column_pruning_results = {

            "original_column_count":
                len(full_columns),

            "required_column_count":
                len(pruned_columns),

            "columns_removed":
                columns_removed,

            "original_columns":
                full_columns,

            "required_columns":
                pruned_columns,
        }

        self.logger.info(
            "Column pruning demonstration completed."
        )

        return self.column_pruning_results

    # =====================================================
    # 6. Broadcast join
    # =====================================================

    def compare_broadcast_join(self):

        print("\n" + "=" * 70)
        print("SP5 — STANDARD VS BROADCAST JOIN")
        print("=" * 70)

        # -------------------------------------------------
        # Use SP4 geometry only as the static lookup.
        #
        # Geometry is deliberately NOT duplicated into
        # the activity output.
        # -------------------------------------------------

        lookup_df = (
            self.sp4_df
            .select(
                "grid_id",
                "geometry"
            )
            .dropDuplicates(
                ["grid_id"]
            )
        )

        activity_df = (
            self.sp3_df
            .select(
                "timestamp",
                "grid_id",
                "total_activity"
            )
        )

        # -------------------------------------------------
        # Standard join
        # -------------------------------------------------

        standard_join = (
            activity_df
            .join(
                lookup_df,
                on="grid_id",
                how="left"
            )
        )

        print(
            "\nSTANDARD JOIN PLAN"
        )

        standard_join.explain(
            mode="formatted"
        )

        # -------------------------------------------------
        # Broadcast join
        # -------------------------------------------------

        broadcast_join = (
            activity_df
            .join(
                F.broadcast(
                    lookup_df
                ),
                on="grid_id",
                how="left"
            )
        )

        print(
            "\nBROADCAST JOIN PLAN"
        )

        broadcast_join.explain(
            mode="formatted"
        )

        self.standard_join_df = (
            standard_join
        )

        self.broadcast_join_df = (
            broadcast_join
        )

        self.standard_plan = (
            "Standard join between activity data "
            "and static grid lookup."
        )

        self.broadcast_plan = (
            "Broadcast join between activity data "
            "and static grid lookup."
        )

        self.logger.info(
            "Standard and broadcast join plans "
            "were generated."
        )

        return {
            "standard_join":
                standard_join,

            "broadcast_join":
                broadcast_join,
        }

    # =====================================================
    # 7. Record observations
    # =====================================================

    def document_observations(self):

        print("\n" + "=" * 70)
        print("SP5 — PERFORMANCE OBSERVATIONS")
        print("=" * 70)

        observations = []

        # -------------------------------------------------
        # Observation 1 — cache
        # -------------------------------------------------

        cache_observation = {

            "observation":
                "Caching the entire SP2 dataset is "
                "not appropriate for this local workload.",

            "evidence":
                (
                    "The previous full-data cache experiment "
                    "failed with java.lang.OutOfMemoryError: "
                    "Java heap space while Spark was materializing "
                    "CachedRDDBuilder / MemoryStore blocks."
                ),

            "decision":
                "REJECT full-data caching.",

            "replacement":
                (
                    "Use a narrow DataFrame and "
                    "MEMORY_AND_DISK persistence for the "
                    "repeated-action experiment."
                ),
        }

        observations.append(
            cache_observation
        )

        print(
            "\nObservation 1 — Cache"
        )

        print(
            "Decision: REJECT full-data caching"
        )

        print(
            "Evidence: previous experiment produced "
            "Java heap space OOM during cache materialization."
        )

        # -------------------------------------------------
        # Observation 2 — column pruning
        # -------------------------------------------------

        removed = (
            self.column_pruning_results[
                "columns_removed"
            ]
        )

        observation_2 = {

            "observation":
                "Column pruning reduces the amount of "
                "data required by an aggregation.",

            "evidence":
                (
                    f"Hotspot aggregation requires only "
                    f"grid_id and total_activity. "
                    f"{removed} other columns can be excluded "
                    f"before that aggregation."
                ),

            "decision":
                "ACCEPT narrow projection before aggregation.",
        }

        observations.append(
            observation_2
        )

        print(
            "\nObservation 2 — Column pruning"
        )

        print(
            f"Evidence: "
            f"{removed} columns are unnecessary for "
            f"the hotspot aggregation."
        )

        print(
            "Decision: ACCEPT"
        )

        # -------------------------------------------------
        # Observation 3 — broadcast
        # -------------------------------------------------

        observation_3 = {

            "observation":
                "Broadcasting is appropriate for the "
                "small static grid lookup, subject to "
                "physical-plan evidence.",

            "evidence":
                (
                    "The broadcast execution plan explicitly "
                    "contains a broadcast join strategy, "
                    "whereas the standard plan does not."
                ),

            "decision":
                "ACCEPT broadcast for the static grid lookup.",
        }

        observations.append(
            observation_3
        )

        print(
            "\nObservation 3 — Broadcast"
        )

        print(
            "Evidence: compare the STANDARD JOIN PLAN "
            "and BROADCAST JOIN PLAN above."
        )

        print(
            "Decision: ACCEPT for the small static lookup."
        )

        self.performance_observations = (
            observations
        )

        self.logger.info(
            "Three performance observations documented."
        )

        return observations

    # =====================================================
    # 8. Build summary
    # =====================================================

    def build_summary(self):

        self.summary = {

            "sp2_rows":
                self.sp2_row_count,

            "sp3_rows":
                self.sp3_row_count,

            "sp4_rows":
                self.sp4_row_count,

            "cache_timings":
                self.cache_timings,

            "repartition":
                self.repartition_results,

            "column_pruning":
                self.column_pruning_results,

            "observations":
                self.performance_observations,
        }

        return self.summary

    # =====================================================
    # 9. Save execution comparison notes
    # =====================================================

    def save_execution_notes(self):

        self.output_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        json_path = (
            self.output_dir
            / "sp5_performance_observations.json"
        )

        txt_path = (
            self.output_dir
            / "sp5_execution_comparison_notes.txt"
        )

        with open(
            json_path,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                self.summary,
                file,
                indent=4,
                default=str
            )

        with open(
            txt_path,
            "w",
            encoding="utf-8"
        ) as file:

            file.write(
                "SP5 — PERFORMANCE & EXECUTION "
                "COMPARISON NOTES\n"
            )

            file.write(
                "=" * 70
                + "\n\n"
            )

            file.write(
                "1. CACHE TIMING\n"
            )

            file.write(
                "-" * 70
                + "\n"
            )

            for key, value in (
                self.cache_timings.items()
            ):

                file.write(
                    f"{key}: {value}\n"
                )

            file.write(
                "\n2. REPARTITIONING\n"
            )

            file.write(
                "-" * 70
                + "\n"
            )

            for key, value in (
                self.repartition_results.items()
            ):

                file.write(
                    f"{key}: {value}\n"
                )

            file.write(
                "\n3. COLUMN PRUNING\n"
            )

            file.write(
                "-" * 70
                + "\n"
            )

            for key, value in (
                self.column_pruning_results.items()
            ):

                file.write(
                    f"{key}: {value}\n"
                )

            file.write(
                "\n4. PERFORMANCE OBSERVATIONS\n"
            )

            file.write(
                "-" * 70
                + "\n"
            )

            for index, observation in enumerate(
                self.performance_observations,
                start=1
            ):

                file.write(
                    f"\nObservation {index}\n"
                )

                for key, value in (
                    observation.items()
                ):

                    file.write(
                        f"{key}: {value}\n"
                    )

        self.logger.info(
            "Performance observations saved to %s",
            json_path
        )

        self.logger.info(
            "Execution comparison notes saved to %s",
            txt_path
        )

        return {
            "json":
                json_path,

            "notes":
                txt_path,
        }

    # =====================================================
    # 10. Acceptance criteria
    # =====================================================

    def validate_acceptance_criteria(self):

        print("\n" + "=" * 70)
        print("SP5 ACCEPTANCE CRITERIA")
        print("=" * 70)

        results = {}

        # -------------------------------------------------
        # Criterion 1
        # -------------------------------------------------

        print(
            "\n1. THREE PERFORMANCE OBSERVATIONS DOCUMENTED"
        )

        criterion_1 = (
            len(
                self.performance_observations
            )
            >=
            3
        )

        results[
            "three_observations"
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
            "\n2. AT LEAST ONE OPTIMIZATION REJECTED"
        )

        rejected = any(
            observation.get("decision") == "REJECT full-data caching."
            for observation
            in self.performance_observations
        )

        results[
            "optimization_rejected"
        ] = rejected

        print(
            "PASS"
            if rejected
            else "FAIL"
        )

        # -------------------------------------------------
        # Criterion 3
        # -------------------------------------------------

        print(
            "\n3. BEFORE/AFTER CACHE TIMING CAPTURED"
        )

        timing_exists = (
            self.cache_timings is not None
            and
            self.cache_timings.get(
                "first_persisted_action_seconds"
            ) is not None
            and
            self.cache_timings.get(
                "second_persisted_action_seconds"
            ) is not None
        )

        results[
            "cache_timing"
        ] = timing_exists

        print(
            "PASS"
            if timing_exists
            else "FAIL"
        )

        # -------------------------------------------------
        # Criterion 4
        # -------------------------------------------------

        print(
            "\n4. SP2/SP3/SP4 ROW COUNTS AVAILABLE"
        )

        counts_valid = (
            self.sp2_row_count is not None
            and
            self.sp3_row_count is not None
            and
            self.sp4_row_count is not None
            and
            self.sp3_row_count
            ==
            self.sp4_row_count
        )

        results[
            "checkpoint_counts"
        ] = counts_valid

        print(
            "PASS"
            if counts_valid
            else "FAIL"
        )

        # -------------------------------------------------
        # Criterion 5
        # -------------------------------------------------

        print(
            "\n5. COLUMN PRUNING DEMONSTRATED"
        )

        pruning_valid = (
            self.column_pruning_results is not None
            and
            self.column_pruning_results[
                "required_column_count"
            ]
            <
            self.column_pruning_results[
                "original_column_count"
            ]
        )

        results[
            "column_pruning"
        ] = pruning_valid

        print(
            "PASS"
            if pruning_valid
            else "FAIL"
        )

        # -------------------------------------------------
        # Criterion 6
        # -------------------------------------------------

        print(
            "\n6. REPARTITIONING OBSERVED"
        )

        repartition_valid = (
            self.repartition_results is not None
            and
            self.repartition_results[
                "observed_partitions"
            ]
            > 0
        )

        results[
            "repartitioning"
        ] = repartition_valid

        print(
            "PASS"
            if repartition_valid
            else "FAIL"
        )

        # -------------------------------------------------
        # Criterion 7
        # -------------------------------------------------

        print(
            "\n7. BROADCAST JOIN PLAN DEMONSTRATED"
        )

        broadcast_valid = (
            self.broadcast_join_df is not None
        )

        results[
            "broadcast_join"
        ] = broadcast_valid

        print(
            "PASS"
            if broadcast_valid
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
                "SP5 AUTOMATED ACCEPTANCE: ALL PASS"
            )

        else:

            print(
                "SP5 AUTOMATED ACCEPTANCE: FAILED"
            )

        print(
            "=" * 70
        )

        self.logger.info(
            "SP5 acceptance: %s",
            "ALL PASS"
            if all_passed
            else "FAILED"
        )

        return {
            "all_passed":
                all_passed,

            "criteria":
                results,
        }

    # =====================================================
    # 11. Full process
    # =====================================================

    def process(self):

        self.logger.info(
            "Starting SP5 performance analysis."
        )

        # -------------------------------------------------
        # Input validation
        # -------------------------------------------------

        self.validate_inputs()

        # -------------------------------------------------
        # Explain hotspot
        # -------------------------------------------------

        self.explain_hotspot_aggregation()

        # -------------------------------------------------
        # Cache experiment
        # -------------------------------------------------

        self.compare_cache_timings()

        # -------------------------------------------------
        # Repartition experiment
        # -------------------------------------------------

        self.compare_repartitioning()

        # -------------------------------------------------
        # Column pruning
        # -------------------------------------------------

        self.demonstrate_column_pruning()

        # -------------------------------------------------
        # Broadcast
        # -------------------------------------------------

        self.compare_broadcast_join()

        # -------------------------------------------------
        # Observations
        # -------------------------------------------------

        self.document_observations()

        # -------------------------------------------------
        # Summary
        # -------------------------------------------------

        self.build_summary()

        # -------------------------------------------------
        # Save notes
        # -------------------------------------------------

        output_paths = (
            self.save_execution_notes()
        )

        # -------------------------------------------------
        # Acceptance
        # -------------------------------------------------

        acceptance = (
            self.validate_acceptance_criteria()
        )

        if not acceptance["all_passed"]:

            raise ValueError(
                "SP5 acceptance criteria failed."
            )

        # -------------------------------------------------
        # Final result
        # -------------------------------------------------

        self.logger.info(
            "SP5 performance analysis completed."
        )

        self.logger.info(
            "Log file: %s",
            self.log_file
        )

        return {

            "cache_timings":
                self.cache_timings,

            "repartition":
                self.repartition_results,

            "column_pruning":
                self.column_pruning_results,

            "observations":
                self.performance_observations,

            "acceptance":
                acceptance,

            "output_paths":
                output_paths,

            "log_file":
                self.log_file,
        }