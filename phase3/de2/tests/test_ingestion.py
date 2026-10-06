from pathlib import Path

from phase3.de2.ingestion import (
    validate_schema,
    validate_minimum_quality,
)


def write_csv(tmp_path: Path, filename: str, content: str) -> Path:
    file_path = tmp_path / filename
    file_path.write_text(content, encoding="utf-8")
    return file_path


def test_valid_schema(tmp_path):
    file_path = write_csv(
        tmp_path,
        "valid.csv",
        """datetime,CellID,countrycode,smsin,smsout,callin,callout,internet
2013-11-01 00:00:00,1,39,10,5,3,2,100
""",
    )

    valid, reason = validate_schema(file_path)

    assert valid is True
    assert reason == "Schema validation passed."


def test_missing_required_column(tmp_path):
    file_path = write_csv(
        tmp_path,
        "missing_column.csv",
        """datetime,CellID,countrycode,smsin,smsout,callin
2013-11-01 00:00:00,1,39,10,5,3
""",
    )

    valid, reason = validate_schema(file_path)

    assert valid is False
    assert "callout" in reason
    assert "internet" in reason


def test_negative_activity_rejected(tmp_path):
    file_path = write_csv(
        tmp_path,
        "negative.csv",
        """datetime,CellID,countrycode,smsin,smsout,callin,callout,internet
2013-11-01 00:00:00,1,39,-10,5,3,2,100
""",
    )

    valid, row_count, reason = validate_minimum_quality(file_path)

    assert valid is False
    assert row_count == 1
    assert "Negative activity values in smsin" in reason


def test_missing_activity_values_allowed(tmp_path):
    file_path = write_csv(
        tmp_path,
        "missing_activity.csv",
        """datetime,CellID,countrycode,smsin,smsout,callin,callout,internet
2013-11-01 00:00:00,1,39,,5,3,2,100
""",
    )

    valid, row_count, reason = validate_minimum_quality(file_path)

    assert valid is True
    assert row_count == 1
    assert reason == "Minimum quality validation passed."


def test_empty_file_rejected(tmp_path):
    file_path = write_csv(
        tmp_path,
        "empty.csv",
        """datetime,CellID,countrycode,smsin,smsout,callin,callout,internet
""",
    )

    valid, row_count, reason = validate_minimum_quality(file_path)

    assert valid is False
    assert row_count == 0
    assert "zero data rows" in reason.lower()