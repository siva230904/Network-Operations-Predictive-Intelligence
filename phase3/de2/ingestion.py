from __future__ import annotations

import csv
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
import re
import pandas as pd

from phase3.de2.config import (
    AUDIT_LOG_FILE,
    INPUT_FILE_PATTERN,
    LANDING_DIR,
    RAW_DIR,
    REJECTED_DIR,
    REQUIRED_COLUMNS,
    ACTIVITY_COLUMNS,
    ALLOW_NEGATIVE_ACTIVITY,
    SKIP_ALREADY_PROCESSED,
    SOURCE_FILENAME_REGEX,
    ensure_directories,
)


# =========================================================
# Logging
# =========================================================

logger = logging.getLogger(__name__)


# =========================================================
# Audit log configuration
# =========================================================

AUDIT_COLUMNS = [
    "filename",
    "status",
    "row_count",
    "reason",
    "processed_at",
]


# =========================================================
# Audit logging
# =========================================================

def write_audit_record(
    filename: str,
    status: str,
    row_count: int,
    reason: str,
) -> None:
    """
    Append one ingestion attempt to the DE2 audit log.

    Required audit metadata:

        filename
        status
        row_count
        reason
        processed_at
    """

    ensure_directories()

    processed_at = datetime.now(
        timezone.utc
    ).isoformat()

    file_exists = AUDIT_LOG_FILE.exists()

    with AUDIT_LOG_FILE.open(
        "a",
        newline="",
        encoding="utf-8",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=AUDIT_COLUMNS,
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow(
            {
                "filename": filename,
                "status": status,
                "row_count": row_count,
                "reason": reason,
                "processed_at": processed_at,
            }
        )

    logger.info(
        "Audit record written: %s | %s | %s",
        filename,
        status,
        reason,
    )


# =========================================================
# Detect files
# =========================================================

def detect_files() -> list[Path]:
    """
    Detect incoming telecom CSV files in the landing zone.

    Only files matching:

        sms-call-internet-mi-*.csv

    are considered DE2 source files.

    Existing raw files are not scanned as landing inputs.
    """

    ensure_directories()

    candidate_files = sorted(
        LANDING_DIR.glob(
            INPUT_FILE_PATTERN
        )
    )

    files = [
        path
        for path in candidate_files
        if (
            path.is_file()
            and re.match(
                SOURCE_FILENAME_REGEX,
                path.name,
            )
        )
    ]

    logger.info(
        "Detected %d landing file(s).",
        len(files),
    )

    for path in files:
        logger.info(
            "Detected: %s",
            path.name,
        )

    return files


# =========================================================
# Validate schema
# =========================================================

def validate_schema(
    file_path: Path,
) -> tuple[bool, str]:
    """
    Validate that the source CSV contains all required
    columns.

    Extra columns are allowed.

    Returns:

        (True, "Schema validation passed.")

    or:

        (False, "reason...")
    """

    try:

        dataframe = pd.read_csv(
            file_path,
            nrows=0,
        )

    except Exception as exc:

        return (
            False,
            f"Unable to read CSV schema: {exc}",
        )

    actual_columns = list(
        dataframe.columns
    )

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in actual_columns
    ]

    if missing_columns:

        return (
            False,
            "Missing required columns: "
            f"{missing_columns}",
        )

    return (
        True,
        "Schema validation passed.",
    )


# =========================================================
# Validate minimum quality
# =========================================================

def validate_minimum_quality(
    file_path: Path,
) -> tuple[bool, int, str]:
    """
    Validate minimum source-data quality.

    Rules:

    1. File must contain at least one row.
    2. datetime must be parseable.
    3. CellID must be present and numeric.
    4. countrycode must be present and numeric.
    5. Populated activity values must be numeric.
    6. Negative activity values are rejected.
    7. Missing activity values are allowed.

    Returns:

        (passed, row_count, reason)
    """

    try:

        dataframe = pd.read_csv(
            file_path
        )

    except Exception as exc:

        return (
            False,
            0,
            f"Unable to read CSV: {exc}",
        )

    row_count = len(dataframe)

    if row_count == 0:

        return (
            False,
            0,
            "File contains zero data rows.",
        )

    # -----------------------------------------------------
    # datetime
    # -----------------------------------------------------

    parsed_datetime = pd.to_datetime(
        dataframe["datetime"],
        errors="coerce",
    )

    invalid_datetime = int(
        parsed_datetime.isna().sum()
    )

    if invalid_datetime:

        return (
            False,
            row_count,
            "Invalid datetime values: "
            f"{invalid_datetime}",
        )

    # -----------------------------------------------------
    # CellID
    # -----------------------------------------------------

    cell_ids = pd.to_numeric(
        dataframe["CellID"],
        errors="coerce",
    )

    invalid_cell_ids = int(
        cell_ids.isna().sum()
    )

    if invalid_cell_ids:

        return (
            False,
            row_count,
            "Invalid or missing CellID values: "
            f"{invalid_cell_ids}",
        )

    # -----------------------------------------------------
    # countrycode
    # -----------------------------------------------------

    country_codes = pd.to_numeric(
        dataframe["countrycode"],
        errors="coerce",
    )

    invalid_country_codes = int(
        country_codes.isna().sum()
    )

    if invalid_country_codes:

        return (
            False,
            row_count,
            "Invalid or missing countrycode values: "
            f"{invalid_country_codes}",
        )

    # -----------------------------------------------------
    # Activity columns
    # -----------------------------------------------------

    for column in ACTIVITY_COLUMNS:

        numeric_values = pd.to_numeric(
            dataframe[column],
            errors="coerce",
        )

        # A source activity column may legitimately
        # contain missing values.
        invalid_non_numeric = (
            dataframe[column].notna()
            & numeric_values.isna()
        )

        invalid_count = int(
            invalid_non_numeric.sum()
        )

        if invalid_count:

            return (
                False,
                row_count,
                f"Invalid non-numeric values in "
                f"{column}: {invalid_count}",
            )

        # -------------------------------------------------
        # Negative values
        # -------------------------------------------------

        if not ALLOW_NEGATIVE_ACTIVITY:

            negative_count = int(
                (
                    numeric_values < 0
                ).sum()
            )

            if negative_count:

                return (
                    False,
                    row_count,
                    f"Negative activity values in "
                    f"{column}: {negative_count}",
                )

    return (
        True,
        row_count,
        "Minimum quality validation passed.",
    )


# =========================================================
# Already processed check
# =========================================================

def already_processed(
    file_path: Path,
) -> bool:
    """
    Determine whether the source filename already exists
    in the raw zone.

    DE2 uses filename-based idempotency for the ingestion
    boundary.
    """

    raw_path = (
        RAW_DIR
        / file_path.name
    )

    return raw_path.exists()


# =========================================================
# Route valid file
# =========================================================

def route_to_raw(
    file_path: Path,
    row_count: int,
) -> str:
    """
    Move an accepted landing file into the raw zone.

    The raw zone is treated as immutable after acceptance.
    """

    destination = (
        RAW_DIR
        / file_path.name
    )

    if destination.exists():

        raise FileExistsError(
            "Raw destination already exists: "
            f"{destination}"
        )

    shutil.move(
        str(file_path),
        str(destination),
    )

    write_audit_record(
        filename=file_path.name,
        status="ACCEPTED",
        row_count=row_count,
        reason="File validated and routed to raw.",
    )

    logger.info(
        "Accepted file routed to raw: %s",
        destination,
    )

    return "ACCEPTED"


# =========================================================
# Route invalid file
# =========================================================

def route_to_rejected(
    file_path: Path,
    row_count: int,
    reason: str,
) -> str:
    """
    Move a failed landing file into rejected and record
    the validation reason.
    """

    destination = (
        REJECTED_DIR
        / file_path.name
    )

    # Avoid overwriting an earlier rejected copy.
    if destination.exists():

        timestamp = datetime.now(
            timezone.utc
        ).strftime(
            "%Y%m%dT%H%M%SZ"
        )

        destination = (
            REJECTED_DIR
            / (
                f"{file_path.stem}"
                f"__{timestamp}"
                f"{file_path.suffix}"
            )
        )

    shutil.move(
        str(file_path),
        str(destination),
    )

    write_audit_record(
        filename=file_path.name,
        status="REJECTED",
        row_count=row_count,
        reason=reason,
    )

    logger.warning(
        "Rejected file routed to: %s",
        destination,
    )

    return "REJECTED"


# =========================================================
# Route one file
# =========================================================

def route_file(
    file_path: Path,
) -> str:
    """
    Validate and route one landing file.

    Possible outcomes:

        ACCEPTED
        REJECTED
        SKIPPED

    Already-processed files are not copied into raw again.
    The attempt is recorded in the audit log.
    """

    file_path = Path(
        file_path
    )

    if not file_path.exists():

        raise FileNotFoundError(
            f"Landing file does not exist: "
            f"{file_path}"
        )

    if not file_path.is_file():

        raise ValueError(
            f"Landing path is not a file: "
            f"{file_path}"
        )

    logger.info(
        "Processing landing file: %s",
        file_path.name,
    )

    # -----------------------------------------------------
    # Idempotency
    # -----------------------------------------------------

    if already_processed(
        file_path
    ):

        if SKIP_ALREADY_PROCESSED:

            write_audit_record(
                filename=file_path.name,
                status="SKIPPED",
                row_count=0,
                reason=(
                    "File with the same filename "
                    "already exists in raw."
                ),
            )

            logger.info(
                "Skipping already processed file: %s",
                file_path.name,
            )

            return "SKIPPED"

        raise FileExistsError(
            "File already exists in raw: "
            f"{file_path.name}"
        )

    # -----------------------------------------------------
    # Schema validation
    # -----------------------------------------------------

    schema_valid, schema_reason = (
        validate_schema(
            file_path
        )
    )

    if not schema_valid:

        return route_to_rejected(
            file_path=file_path,
            row_count=0,
            reason=schema_reason,
        )

    # -----------------------------------------------------
    # Minimum quality validation
    # -----------------------------------------------------

    quality_valid, row_count, quality_reason = (
        validate_minimum_quality(
            file_path
        )
    )

    if not quality_valid:

        return route_to_rejected(
            file_path=file_path,
            row_count=row_count,
            reason=quality_reason,
        )

    # -----------------------------------------------------
    # Accepted
    # -----------------------------------------------------

    return route_to_raw(
        file_path=file_path,
        row_count=row_count,
    )


# =========================================================
# Process all landing files
# =========================================================

def process_landing_files() -> dict[str, int]:
    """
    Detect and process every matching landing file.

    Returns a summary:

        {
            "detected": N,
            "accepted": N,
            "rejected": N,
            "skipped": N
        }
    """

    files = detect_files()

    summary = {
        "detected": len(files),
        "accepted": 0,
        "rejected": 0,
        "skipped": 0,
    }

    for file_path in files:

        try:

            status = route_file(
                file_path
            )

            if status == "ACCEPTED":
                summary["accepted"] += 1

            elif status == "REJECTED":
                summary["rejected"] += 1

            elif status == "SKIPPED":
                summary["skipped"] += 1

        except Exception as exc:

            logger.exception(
                "Unexpected ingestion failure for %s",
                file_path.name,
            )

            write_audit_record(
                filename=file_path.name,
                status="FAILED",
                row_count=0,
                reason=str(exc),
            )

            raise

    logger.info(
        "DE2 ingestion summary: %s",
        summary,
    )

    return summary


# =========================================================
# CLI
# =========================================================

def configure_logging() -> None:

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        ),
    )


if __name__ == "__main__":

    configure_logging()

    summary = process_landing_files()

    print()
    print("=" * 60)
    print("DE2 LANDING → RAW INGESTION")
    print("=" * 60)

    print(
        f"Detected:  {summary['detected']:,}"
    )

    print(
        f"Accepted:  {summary['accepted']:,}"
    )

    print(
        f"Rejected:  {summary['rejected']:,}"
    )

    print(
        f"Skipped:   {summary['skipped']:,}"
    )

    print()
    print(
        f"Audit log: {AUDIT_LOG_FILE}"
    )

    print("=" * 60)