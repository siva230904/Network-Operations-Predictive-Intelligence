from pathlib import Path
import pandas as pd

from phase4.api.config import NP3_ALERT_PATH


def main():
    print("NP3 file:")
    print(NP3_ALERT_PATH)

    np3 = pd.read_csv(NP3_ALERT_PATH)

    print()
    print("NP3 columns:")
    print(list(np3.columns))

    print()
    print("NP3 row count:")
    print(len(np3))

    print()
    print("NP3 timestamp sample BEFORE conversion:")
    print(np3["timestamp"].head(10).to_string())

    np3["timestamp"] = pd.to_datetime(
        np3["timestamp"],
        utc=True,
    ).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)

    print()
    print("NP3 timestamp range:")
    print("min:", np3["timestamp"].min())
    print("max:", np3["timestamp"].max())

    print()
    print("Invalid NP3 timestamps:")
    print(np3["timestamp"].isna().sum())

    print()
    print("NP3 unique dates:")
    print(
        np3["timestamp"]
        .dt.date
        .value_counts()
        .sort_index()
        .to_string()
    )

    print()
    print("NP3 sample after conversion:")
    print(
        np3[
            ["grid_id", "timestamp", "alert_type"]
        ].head(20).to_string(index=False)
    )


if __name__ == "__main__":
    main()