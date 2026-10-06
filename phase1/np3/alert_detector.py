import logging
from pathlib import Path
from datetime import datetime

import pandas as pd


# =========================================================
# Network Activity Alert Generator
# =========================================================

class NetworkAlertGenerator:
    """
    Generate transparent rule-based network activity alerts.

    Input:
        One-record-per-grid/hour analytics CSV produced by NP2.

    Input grain:
        timestamp + grid_id

    Rules:
        HIGH_ACTIVITY
        ACTIVITY_SPIKE
        ACTIVITY_DROP

    Baseline:
        Leave-one-out median of the same grid's
        within-day activity values.
    """

    # -----------------------------------------------------
    # Constructor
    # -----------------------------------------------------

    def __init__(
        self,
        file_path=None,
        dataframe=None,
        high_activity_ratio=2.0,
        spike_ratio=1.5,
        drop_ratio=0.5,
        activity_floor=None,
        log_dir="data/logs",
    ):

        if file_path is None and dataframe is None:
            raise ValueError(
                "Provide either file_path or dataframe."
            )

        if file_path is not None and dataframe is not None:
            raise ValueError(
                "Provide either file_path or dataframe, "
                "not both."
            )

        self.file_path = file_path

        if dataframe is not None:
            self.dataframe = dataframe.copy()
        else:
            self.dataframe = None

        # -------------------------------------------------
        # Thresholds
        # -------------------------------------------------

        self.high_activity_ratio = float(
            high_activity_ratio
        )

        self.spike_ratio = float(
            spike_ratio
        )

        self.drop_ratio = float(
            drop_ratio
        )

        self.activity_floor = (
            None
            if activity_floor is None
            else float(activity_floor)
        )

        # -------------------------------------------------
        # Runtime state
        # -------------------------------------------------

        self.analytics_data = None
        self.alert_data = None
        self.alert_summary = None

        # -------------------------------------------------
        # Logging
        # -------------------------------------------------

        log_path = Path(log_dir)

        log_path.mkdir(
            parents=True,
            exist_ok=True
        )

        timestamp = datetime.now().strftime(
            "%Y%m%d_%H%M%S_%f"
        )

        if file_path is not None:
            input_stem = Path(file_path).stem
        else:
            input_stem = "dataframe_input"

        self.log_file = (
            log_path /
            f"{input_stem}_alerts_{timestamp}.log"
        )

        self.logger = logging.getLogger(
            f"NetworkAlertGenerator.{id(self)}"
        )

        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )

        file_handler = logging.FileHandler(
            self.log_file,
            mode="a"
        )

        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()

        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    # =====================================================
    # 1. Load data
    # =====================================================

    def load_data(self):
        """
        Load the grid/hour analytics CSV.
        """

        if self.file_path is not None:

            path = Path(self.file_path)

            self.logger.info(
                "Loading analytics file: %s",
                path
            )

            if not path.exists():
                raise FileNotFoundError(
                    f"Analytics file not found: {path}"
                )

            if path.suffix.lower() != ".csv":
                raise ValueError(
                    "NetworkAlertGenerator expects "
                    "a CSV file."
                )

            self.analytics_data = pd.read_csv(
                path
            )

        else:

            self.logger.info(
                "Using supplied analytics DataFrame."
            )

            self.analytics_data = (
                self.dataframe.copy()
            )

        self.logger.info(
            "Loaded %d analytics rows.",
            len(self.analytics_data)
        )

        return self.analytics_data

    # =====================================================
    # 2. Validate input
    # =====================================================

    def validate_input(self):
        """
        Validate the NP2 grid/hour analytics data.
        """

        required_columns = [
            "timestamp",
            "grid_id",
            "total_activity",
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in self.analytics_data.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing required columns: "
                f"{missing_columns}"
            )

        # -------------------------------------------------
        # Timestamp
        # -------------------------------------------------

        self.analytics_data["timestamp"] = (
            pd.to_datetime(
                self.analytics_data["timestamp"],
                errors="coerce"
            )
        )

        if self.analytics_data["timestamp"].isna().any():

            count = int(
                self.analytics_data[
                    "timestamp"
                ].isna().sum()
            )

            raise ValueError(
                f"Found {count} invalid timestamps."
            )

        # -------------------------------------------------
        # Grid ID
        # -------------------------------------------------

        if self.analytics_data["grid_id"].isna().any():

            raise ValueError(
                "analytics_data contains "
                "missing grid_id values."
            )

        invalid_grids = self.analytics_data[
            (self.analytics_data["grid_id"] < 1) |
            (self.analytics_data["grid_id"] > 10000)
        ]

        if not invalid_grids.empty:

            raise ValueError(
                "analytics_data contains grid_id "
                "values outside 1-10000."
            )

        # -------------------------------------------------
        # Duplicate grid/hour check
        # -------------------------------------------------

        duplicate_count = int(
            self.analytics_data
            .duplicated(
                subset=[
                    "timestamp",
                    "grid_id"
                ]
            )
            .sum()
        )

        if duplicate_count > 0:

            raise ValueError(
                f"Found {duplicate_count} duplicate "
                "(timestamp, grid_id) records."
            )

        # -------------------------------------------------
        # Activity validation
        # -------------------------------------------------

        if (
            self.analytics_data[
                "total_activity"
            ]
            .isna()
            .any()
        ):

            raise ValueError(
                "total_activity contains "
                "missing values."
            )

        if (
            self.analytics_data[
                "total_activity"
            ] < 0
        ).any():

            raise ValueError(
                "total_activity contains "
                "negative values."
            )

        self.logger.info(
            "Input validation passed."
        )

        return True

    # =====================================================
    # 3. Determine activity floor
    # =====================================================

    def determine_activity_floor(self):
        """
        Determine activity floor.

        Default:
            10th percentile of daily grid totals.
        """

        if self.activity_floor is not None:

            self.logger.info(
                "Using configured activity floor: %.4f",
                self.activity_floor
            )

            return self.activity_floor

        daily_grid_activity = (
            self.analytics_data
            .groupby("grid_id")[
                "total_activity"
            ]
            .sum()
        )

        self.activity_floor = float(
            daily_grid_activity.quantile(
                0.10
            )
        )

        self.logger.info(
            "Calculated activity floor "
            "from 10th percentile: %.4f",
            self.activity_floor
        )

        return self.activity_floor

    # =====================================================
    # 4. Build exact leave-one-out median baseline
    # =====================================================
    #third version for ml
    def build_baseline(
        self,
        bucket_columns=None,
    ):
        """
        Build a leave-one-out median baseline using
        configurable grouping/bucketing columns.

        NP3 default:
            bucket_columns=["grid_id"]

        ML4:
            bucket_columns=["grid_id", "hour_of_day"]

        The current observation is excluded from its
        own baseline.

        Uses a vectorized sorted-rank approach and avoids
        row-by-row Python loops.
        """

        if bucket_columns is None:
            bucket_columns = ["grid_id"]

        if not isinstance(bucket_columns, list):
            raise TypeError(
                "bucket_columns must be a list."
            )

        if not bucket_columns:
            raise ValueError(
                "bucket_columns cannot be empty."
            )

        missing_columns = [
            column
            for column in bucket_columns
            if column not in self.analytics_data.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing baseline bucket columns: "
                f"{missing_columns}"
            )

        # -----------------------------------------------------
        # Prepare data
        # -----------------------------------------------------

        df = (
            self.analytics_data
            .copy()
            .sort_values(
                bucket_columns + ["timestamp"]
            )
            .reset_index(drop=True)
        )

        # -----------------------------------------------------
        # Number of observations in each bucket
        # -----------------------------------------------------

        group_sizes = (
            df.groupby(
                bucket_columns,
                sort=False
            )["total_activity"]
            .transform("size")
        )

        if (group_sizes < 2).any():
            raise ValueError(
                "Cannot calculate leave-one-out median "
                "for a bucket with fewer than 2 observations."
            )

        # -----------------------------------------------------
        # Rank activity inside each bucket.
        #
        # method="first" ensures unique ranks when values tie.
        # -----------------------------------------------------

        df["_activity_rank"] = (
            df.groupby(
                bucket_columns,
                sort=False
            )["total_activity"]
            .rank(
                method="first",
                ascending=True
            )
            .astype("int64")
        )

        # -----------------------------------------------------
        # Create sorted lookup table.
        #
        # Each row represents:
        #
        # bucket + rank -> activity value
        # -----------------------------------------------------

        lookup = (
            df[
                bucket_columns
                + [
                    "_activity_rank",
                    "total_activity",
                ]
            ]
            .rename(
                columns={
                    "_activity_rank": "_lookup_rank",
                    "total_activity": "_lookup_value",
                }
            )
        )

        # -----------------------------------------------------
        # For leave-one-out median:
        #
        # After removing the current observation:
        #
        # remaining_n = n - 1
        #
        # Lower and upper median positions are calculated
        # separately so both odd and even bucket sizes work.
        # -----------------------------------------------------

        remaining_n = group_sizes - 1

        lower_position = (
            (remaining_n + 1) // 2
        )

        upper_position = (
            (remaining_n + 2) // 2
        )

        # -----------------------------------------------------
        # If the removed observation is before or at a median
        # position, the lookup rank moves one place right.
        # -----------------------------------------------------

        lower_lookup_rank = (
            lower_position
            + (
                df["_activity_rank"]
                <= lower_position
            ).astype("int64")
        )

        upper_lookup_rank = (
            upper_position
            + (
                df["_activity_rank"]
                <= upper_position
            ).astype("int64")
        )

        # -----------------------------------------------------
        # Store lookup ranks temporarily.
        # -----------------------------------------------------

        df["_lower_lookup_rank"] = (
            lower_lookup_rank
        )

        df["_upper_lookup_rank"] = (
            upper_lookup_rank
        )

        # -----------------------------------------------------
        # Lower median lookup
        # -----------------------------------------------------

        lower_lookup = lookup.rename(
            columns={
                "_lookup_rank":
                    "_lower_lookup_rank",
                "_lookup_value":
                    "_lower_value",
            }
        )

        df = df.merge(
            lower_lookup,
            on=(
                bucket_columns
                + ["_lower_lookup_rank"]
            ),
            how="left",
            sort=False,
        )

        # -----------------------------------------------------
        # Upper median lookup
        # -----------------------------------------------------

        upper_lookup = lookup.rename(
            columns={
                "_lookup_rank":
                    "_upper_lookup_rank",
                "_lookup_value":
                    "_upper_value",
            }
        )

        df = df.merge(
            upper_lookup,
            on=(
                bucket_columns
                + ["_upper_lookup_rank"]
            ),
            how="left",
            sort=False,
        )

        # -----------------------------------------------------
        # Calculate median.
        #
        # For odd number of remaining observations:
        # lower == upper, so this returns that value.
        #
        # For even number:
        # average of the two middle values.
        # -----------------------------------------------------

        df["baseline_activity"] = (
            (
                df["_lower_value"]
                +
                df["_upper_value"]
            )
            / 2.0
        )

        # -----------------------------------------------------
        # Validate result
        # -----------------------------------------------------

        if df["baseline_activity"].isna().any():
            raise ValueError(
                "Baseline calculation produced "
                "missing values."
            )

        if (
            ~pd.Series(
                df["baseline_activity"]
            ).apply(pd.api.types.is_number)
        ).any():
            raise ValueError(
                "Baseline calculation produced "
                "non-numeric values."
            )

        # -----------------------------------------------------
        # Remove temporary columns.
        # -----------------------------------------------------

        df = df.drop(
            columns=[
                "_activity_rank",
                "_lower_lookup_rank",
                "_upper_lookup_rank",
                "_lower_value",
                "_upper_value",
            ],
            errors="ignore",
        )

        self.analytics_data = df

        self.logger.info(
            "Built leave-one-out median baseline "
            "using buckets: %s",
            bucket_columns,
        )

        return self.analytics_data
    #second version of build_baseline() using pandas only.  This is simpler but slower than the NumPy version above.  It is currently used in the current pipeline.
    # def build_baseline(self):
    #     """
    #     Build the exact leave-one-out within-day median baseline.

    #     For each grid/hour:

    #         baseline =
    #         median(total_activity for the same grid,
    #         excluding the current hour)

    #     Uses pandas only.
    #     """

    #     df = (
    #         self.analytics_data
    #         .copy()
    #         .sort_values(
    #             ["grid_id", "timestamp"]
    #         )
    #         .reset_index(drop=True)
    #     )

    #     # -----------------------------------------------------
    #     # Validate that every grid has enough observations
    #     # -----------------------------------------------------

    #     group_sizes = (
    #         df.groupby("grid_id")[
    #             "total_activity"
    #         ]
    #         .transform("size")
    #     )

    #     if (group_sizes < 2).any():

    #         raise ValueError(
    #             "Cannot calculate leave-one-out "
    #             "median for a grid with fewer "
    #             "than 2 observations."
    #         )

    #     # -----------------------------------------------------
    #     # Rank activity values within each grid.
    #     #
    #     # method="first" gives every row a unique position,
    #     # even when two activity values are identical.
    #     # -----------------------------------------------------

    #     df["_activity_rank"] = (
    #         df.groupby("grid_id")[
    #             "total_activity"
    #         ]
    #         .rank(
    #             method="first",
    #             ascending=True
    #         )
    #         .astype(int)
    #     )

    #     # -----------------------------------------------------
    #     # For 24 observations:
    #     #
    #     # After removing one observation, 23 remain.
    #     #
    #     # The median is the 12th value.
    #     #
    #     # 1-based position = 12
    #     # 0-based position = 11
    #     # -----------------------------------------------------

    #     n = group_sizes

    #     median_position = (
    #         (n - 2) // 2
    #     ) + 1

    #     # -----------------------------------------------------
    #     # Determine which sorted position should be used
    #     # after removing the current observation.
    #     #
    #     # If current rank is <= median position,
    #     # the median shifts one position to the right.
    #     # Otherwise it stays where it is.
    #     # -----------------------------------------------------

    #     df["_baseline_rank"] = (
    #         median_position
    #         + (
    #             df["_activity_rank"]
    #             <= median_position
    #         ).astype(int)
    #     )

    #     # -----------------------------------------------------
    #     # Create lookup table:
    #     #
    #     # grid_id + rank → activity
    #     # -----------------------------------------------------

    #     lookup = (
    #         df[
    #             [
    #                 "grid_id",
    #                 "_activity_rank",
    #                 "total_activity"
    #             ]
    #         ]
    #         .rename(
    #             columns={
    #                 "_activity_rank":
    #                     "_baseline_rank",
    #                 "total_activity":
    #                     "_baseline_value"
    #             }
    #         )
    #     )

    #     # -----------------------------------------------------
    #     # Join the required replacement value.
    #     # -----------------------------------------------------

    #     df = df.merge(
    #         lookup,
    #         on=[
    #             "grid_id",
    #             "_baseline_rank"
    #         ],
    #         how="left",
    #         sort=False
    #     )

    #     # -----------------------------------------------------
    #     # Store baseline
    #     # -----------------------------------------------------

    #     df["baseline_activity"] = (
    #         df["_baseline_value"]
    #     )

    #     # -----------------------------------------------------
    #     # Remove temporary columns
    #     # -----------------------------------------------------

    #     df = df.drop(
    #         columns=[
    #             "_activity_rank",
    #             "_baseline_rank",
    #             "_baseline_value"
    #         ]
    #     )

    #     self.analytics_data = df

    #     self.logger.info(
    #         "Built exact leave-one-out "
    #         "within-day median baseline."
    #     )

    #     return self.analytics_data

    #first version of build_baseline() using NumPy for speed.  This is more complex but faster than the pandas-only version above.  It is currently commented out because it is not being used in the current pipeline.
    # def build_baseline(self):
    #     """
    #     Build the exact leave-one-out within-day median baseline.

    #     For each grid/hour:

    #         baseline =
    #         median(total_activity for the same grid,
    #         excluding the current hour)

    #     The current observation is never included
    #     in its own baseline.
    #     """

    #     df = (
    #         self.analytics_data
    #         .copy()
    #         .sort_values(
    #             ["grid_id", "timestamp"]
    #         )
    #         .reset_index(drop=True)
    #     )

    #     # -----------------------------------------------------
    #     # Work with NumPy arrays for speed
    #     # -----------------------------------------------------

    #     values = (
    #         df["total_activity"]
    #         .to_numpy(dtype=float)
    #     )

    #     grid_values = (
    #         df["grid_id"]
    #         .to_numpy()
    #     )

    #     baselines = np.empty(
    #         len(df),
    #         dtype=float
    #     )

    #     # -----------------------------------------------------
    #     # Process each grid once.
    #     #
    #     # There are 10,000 grids and 24 hours per grid,
    #     # so this is only 10,000 small operations instead
    #     # of 240,000 DataFrame operations.
    #     # -----------------------------------------------------

    #     unique_grids, start_positions = np.unique(
    #         grid_values,
    #         return_index=True
    #     )

    #     end_positions = np.append(
    #         start_positions[1:],
    #         len(df)
    #     )

    #     for start, end in zip(
    #         start_positions,
    #         end_positions
    #     ):

    #         grid_activity = values[
    #             start:end
    #         ]

    #         # -------------------------------------------------
    #         # Sort once.
    #         # -------------------------------------------------

    #         sorted_activity = np.sort(
    #             grid_activity
    #         )

    #         n = len(
    #             sorted_activity
    #         )

    #         if n < 2:
    #             raise ValueError(
    #                 "Cannot calculate leave-one-out "
    #                 "median for a grid with fewer "
    #                 "than 2 observations."
    #             )

    #         # -------------------------------------------------
    #         # For n = 24:
    #         #
    #         # Remove one observation → 23 remain.
    #         #
    #         # Median position = 11 (zero-based).
    #         #
    #         # If the removed value occurs at a position
    #         # <= 11 in the sorted array, the new median
    #         # moves to position 12.
    #         #
    #         # Otherwise it remains at position 11.
    #         # -------------------------------------------------

    #         median_position = (
    #             (n - 2) // 2
    #         )

    #         # -------------------------------------------------
    #         # Find the sorted position of each observation.
    #         #
    #         # argsort gives positions in the original
    #         # grid_activity array.
    #         # -------------------------------------------------

    #         sort_order = np.argsort(
    #             grid_activity,
    #             kind="mergesort"
    #         )

    #         ranks = np.empty(
    #             n,
    #             dtype=int
    #         )

    #         ranks[sort_order] = np.arange(n)

    #         # -------------------------------------------------
    #         # Calculate leave-one-out median for each row.
    #         # -------------------------------------------------

    #         for local_position in range(n):

    #             if (
    #                 ranks[local_position]
    #                 <= median_position
    #             ):

    #                 replacement_position = (
    #                     median_position + 1
    #                 )

    #             else:

    #                 replacement_position = (
    #                     median_position
    #                 )

    #             baselines[
    #                 start + local_position
    #             ] = sorted_activity[
    #                 replacement_position
    #             ]

    #     # -----------------------------------------------------
    #     # Store baseline
    #     # -----------------------------------------------------

    #     df["baseline_activity"] = baselines

    #     self.analytics_data = df

    #     self.logger.info(
    #         "Built exact leave-one-out "
    #         "within-day median baseline."
    #     )

    #     return self.analytics_data

    # =====================================================
    # 5. Generate alerts
    # =====================================================

    def generate_alerts(self):
        """
        Generate alerts using vectorized operations.

        The business rules are unchanged.
        """

        if (
            "baseline_activity"
            not in self.analytics_data.columns
        ):
            raise RuntimeError(
                "Run build_baseline() first."
            )

        df = (
            self.analytics_data
            .copy()
            .sort_values(
                ["grid_id", "timestamp"]
            )
            .reset_index(drop=True)
        )

        # -------------------------------------------------
        # Previous hour
        # -------------------------------------------------

        df["previous_activity"] = (
            df.groupby("grid_id")[
                "total_activity"
            ]
            .shift(1)
        )

        # -------------------------------------------------
        # Daily grid total
        #
        # transform() broadcasts the group total to
        # every row instead of repeatedly filtering.
        # -------------------------------------------------

        df["daily_grid_total"] = (
            df.groupby("grid_id")[
                "total_activity"
            ]
            .transform("sum")
        )

        # -------------------------------------------------
        # Common eligibility condition
        # -------------------------------------------------

        eligible = (
            (df["daily_grid_total"]
             >= self.activity_floor)
            &
            (df["baseline_activity"] > 0)
        )

        # =================================================
        # HIGH_ACTIVITY
        # =================================================

        high_mask = (
            eligible
            &
            (
                df["total_activity"]
                >=
                df["baseline_activity"]
                * self.high_activity_ratio
            )
        )

        high = df.loc[
            high_mask,
            [
                "grid_id",
                "timestamp",
                "total_activity",
                "baseline_activity",
            ]
        ].copy()

        high["alert_type"] = (
            "HIGH_ACTIVITY"
        )

        high["reason"] = (
            "HIGH_ACTIVITY: current activity "
            + high["total_activity"].round(2).astype(str)
            + " is at least "
            + str(self.high_activity_ratio)
            + "x the baseline "
            + high["baseline_activity"].round(2).astype(str)
            + "."
        )

        # =================================================
        # ACTIVITY_DROP
        # =================================================

        drop_mask = (
            eligible
            &
            (
                df["total_activity"]
                <=
                df["baseline_activity"]
                * self.drop_ratio
            )
        )

        drop = df.loc[
            drop_mask,
            [
                "grid_id",
                "timestamp",
                "total_activity",
                "baseline_activity",
            ]
        ].copy()

        drop["alert_type"] = (
            "ACTIVITY_DROP"
        )

        drop["reason"] = (
            "ACTIVITY_DROP: current activity "
            + drop["total_activity"].round(2).astype(str)
            + " is at or below "
            + str(self.drop_ratio)
            + "x the baseline "
            + drop["baseline_activity"].round(2).astype(str)
            + "."
        )

        # =================================================
        # ACTIVITY_SPIKE
        # =================================================

        spike_mask = (
            eligible
            &
            df["previous_activity"].notna()
            &
            (df["previous_activity"] > 0)
            &
            (
                df["total_activity"]
                >=
                df["previous_activity"]
                * self.spike_ratio
            )
        )

        spike = df.loc[
            spike_mask,
            [
                "grid_id",
                "timestamp",
                "total_activity",
                "baseline_activity",
                "previous_activity",
            ]
        ].copy()

        spike["alert_type"] = (
            "ACTIVITY_SPIKE"
        )

        spike["reason"] = (
            "ACTIVITY_SPIKE: current activity "
            + spike["total_activity"].round(2).astype(str)
            + " is at least "
            + str(self.spike_ratio)
            + "x the preceding hour "
            + spike["previous_activity"].round(2).astype(str)
            + "."
        )

        # =================================================
        # Combine
        # =================================================

        alerts = pd.concat(
            [
                high,
                drop,
                spike,
            ],
            ignore_index=True
        )

        self.alert_data = alerts[
            [
                "grid_id",
                "timestamp",
                "alert_type",
                "total_activity",
                "baseline_activity",
                "reason",
            ]
        ].rename(
            columns={
                "total_activity":
                    "current_activity"
            }
        )

        self.logger.info(
            "Generated %d alert records.",
            len(self.alert_data)
        )

        return self.alert_data

    # =====================================================
    # 6. Create summary
    # =====================================================

    def create_summary(self):
        """
        Create operational alert summary.
        """

        total_grid_hours = len(
            self.analytics_data
        )

        total_alerts = len(
            self.alert_data
        )

        if self.alert_data.empty:

            alerts_by_type = {}
            top_ten_grids = {}

        else:

            alerts_by_type = (
                self.alert_data[
                    "alert_type"
                ]
                .value_counts()
                .to_dict()
            )

            top_ten_grids = (
                self.alert_data[
                    "grid_id"
                ]
                .value_counts()
                .head(10)
                .to_dict()
            )

        alert_proportion = (
            total_alerts / total_grid_hours
            if total_grid_hours > 0
            else 0.0
        )

        self.alert_summary = {
            "total_grid_hours":
                total_grid_hours,

            "total_alerts":
                total_alerts,

            "alert_proportion":
                alert_proportion,

            "alerts_by_type":
                alerts_by_type,

            "top_ten_grids":
                top_ten_grids,

            "activity_floor":
                self.activity_floor,

            "high_activity_ratio":
                self.high_activity_ratio,

            "spike_ratio":
                self.spike_ratio,

            "drop_ratio":
                self.drop_ratio,
        }

        self.logger.info(
            "Total grid/hours: %d",
            total_grid_hours
        )

        self.logger.info(
            "Total alerts: %d",
            total_alerts
        )

        self.logger.info(
            "Alert proportion: %.2f%%",
            alert_proportion * 100
        )

        self.logger.info(
            "Alerts by type: %s",
            alerts_by_type
        )

        self.logger.info(
            "Top ten alert grids: %s",
            top_ten_grids
        )

        return self.alert_summary

    # =====================================================
    # 7. Export alerts
    # =====================================================

    def export_alerts(
        self,
        output_dir="data/processed",
        filename="network_alerts.csv"
    ):
        """
        Export alert records to CSV.
        """

        output_path = Path(
            output_dir
        )

        output_path.mkdir(
            parents=True,
            exist_ok=True
        )

        output_file = (
            output_path / filename
        )

        self.alert_data.to_csv(
            output_file,
            index=False
        )

        self.logger.info(
            "Alert file exported to: %s",
            output_file
        )

        return output_file

    # =====================================================
    # 8. Print summary
    # =====================================================

    def print_summary(self):

        if self.alert_summary is None:
            raise RuntimeError(
                "Run create_summary() first."
            )

        print("\n" + "=" * 60)
        print("NETWORK ACTIVITY ALERT SUMMARY")
        print("=" * 60)

        print(
            f"Total grid/hours: "
            f"{self.alert_summary['total_grid_hours']:,}"
        )

        print(
            f"Total alerts: "
            f"{self.alert_summary['total_alerts']:,}"
        )

        print(
            f"Alert proportion: "
            f"{self.alert_summary['alert_proportion']:.2%}"
        )

        print(
            f"Activity floor: "
            f"{self.alert_summary['activity_floor']:.2f}"
        )

        print("\nAlerts by type:")

        for alert_type, count in (
            self.alert_summary[
                "alerts_by_type"
            ].items()
        ):
            print(
                f"  {alert_type}: {count:,}"
            )

        print("\nTop ten grids by alert count:")

        for grid_id, count in (
            self.alert_summary[
                "top_ten_grids"
            ].items()
        ):
            print(
                f"  Grid {grid_id}: {count:,}"
            )

        print("=" * 60)

    # =====================================================
    # 9. Full pipeline
    # =====================================================

    def process(
        self,
        output_dir="data/processed",
        filename="network_alerts.csv"
    ):
        """
        Run the complete NP3 pipeline.
        """

        self.logger.info(
            "Starting network activity alert pipeline."
        )

        self.load_data()

        self.validate_input()

        self.determine_activity_floor()

        self.build_baseline()

        self.generate_alerts()

        self.create_summary()

        alert_path = self.export_alerts(
            output_dir=output_dir,
            filename=filename
        )

        self.print_summary()

        self.logger.info(
            "Network activity alert pipeline "
            "completed successfully."
        )

        self.logger.info(
            "Log file: %s",
            self.log_file
        )

        return {
            "alert_path": alert_path,
            "log_path": self.log_file,
            "summary": self.alert_summary,
        }