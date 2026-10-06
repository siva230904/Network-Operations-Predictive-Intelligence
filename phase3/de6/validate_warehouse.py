# =========================================================
# DE6 — Warehouse Validation
# File: phase3/de6/validate_warehouse.py
# =========================================================

import mysql.connector

from config import (
    EXPECTED_GRID_COUNT,
    MYSQL_CONNECT_TIMEOUT,
    MYSQL_DATABASE,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_USER,
)


# =========================================================
# Connection
# =========================================================

def get_connection():

    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        connection_timeout=MYSQL_CONNECT_TIMEOUT,
    )


# =========================================================
# Validation
# =========================================================

def validate():

    connection = get_connection()

    cursor = connection.cursor()

    results = {}

    print()
    print("=" * 70)
    print("DE6 MYSQL WAREHOUSE VALIDATION")
    print("=" * 70)

    # =====================================================
    # 1. Table existence
    # =====================================================

    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_name IN (
            'dim_time',
            'dim_grid',
            'fact_network_activity'
        )
        ORDER BY table_name
        """,
        (MYSQL_DATABASE,)
    )

    tables = [
        row[0]
        for row in cursor.fetchall()
    ]

    required_tables = {
        "dim_time",
        "dim_grid",
        "fact_network_activity",
    }

    criterion_1 = (
        set(tables)
        ==
        required_tables
    )

    results[
        "required_tables"
    ] = criterion_1

    print()
    print("1. REQUIRED TABLES")

    for table in sorted(tables):
        print(f"   {table}")

    print(
        "PASS"
        if criterion_1
        else "FAIL"
    )

    # =====================================================
    # 2. dim_grid count
    # =====================================================

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM dim_grid
        """
    )

    grid_count = cursor.fetchone()[0]

    cursor.execute(
        """
        SELECT COUNT(DISTINCT grid_id)
        FROM dim_grid
        """
    )

    distinct_grid_count = (
        cursor.fetchone()[0]
    )

    criterion_2 = (
        grid_count
        ==
        EXPECTED_GRID_COUNT
        and
        distinct_grid_count
        ==
        grid_count
    )

    results[
        "dim_grid"
    ] = criterion_2

    print()
    print("2. DIM_GRID")

    print(
        f"   Rows: {grid_count:,}"
    )

    print(
        f"   Distinct grid IDs: "
        f"{distinct_grid_count:,}"
    )

    print(
        "PASS"
        if criterion_2
        else "FAIL"
    )

    # =====================================================
    # 3. Fact geometry check
    # =====================================================

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = %s
        AND table_name = 'fact_network_activity'
        AND column_name IN (
            'geometry',
            'geometry_reference',
            'latitude',
            'longitude'
        )
        """,
        (MYSQL_DATABASE,)
    )

    geometry_columns = (
        cursor.fetchone()[0]
    )

    criterion_3 = (
        geometry_columns == 0
    )

    results[
        "fact_has_no_geometry"
    ] = criterion_3

    print()
    print(
        "3. FACT CONTAINS NO GEOMETRY"
    )

    print(
        f"   Geometry-related columns: "
        f"{geometry_columns}"
    )

    print(
        "PASS"
        if criterion_3
        else "FAIL"
    )

    # =====================================================
    # 4. Fact duplicate check
    # =====================================================

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT
                time_key,
                grid_key,
                COUNT(*) AS row_count
            FROM fact_network_activity
            GROUP BY
                time_key,
                grid_key
            HAVING COUNT(*) > 1
        ) duplicates
        """
    )

    duplicate_count = (
        cursor.fetchone()[0]
    )

    criterion_4 = (
        duplicate_count == 0
    )

    results[
        "fact_no_duplicates"
    ] = criterion_4

    print()
    print(
        "4. FACT DUPLICATE KEY CHECK"
    )

    print(
        f"   Duplicate groups: "
        f"{duplicate_count}"
    )

    print(
        "PASS"
        if criterion_4
        else "FAIL"
    )

    # =====================================================
    # 5. Foreign key integrity
    # =====================================================

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM fact_network_activity f
        LEFT JOIN dim_time t
            ON f.time_key = t.time_key
        WHERE t.time_key IS NULL
        """
    )

    orphan_time = (
        cursor.fetchone()[0]
    )

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM fact_network_activity f
        LEFT JOIN dim_grid g
            ON f.grid_key = g.grid_key
        WHERE g.grid_key IS NULL
        """
    )

    orphan_grid = (
        cursor.fetchone()[0]
    )

    criterion_5 = (
        orphan_time == 0
        and
        orphan_grid == 0
    )

    results[
        "foreign_keys"
    ] = criterion_5

    print()
    print(
        "5. FOREIGN KEY INTEGRITY"
    )

    print(
        f"   Orphan time rows: "
        f"{orphan_time}"
    )

    print(
        f"   Orphan grid rows: "
        f"{orphan_grid}"
    )

    print(
        "PASS"
        if criterion_5
        else "FAIL"
    )

    # =====================================================
    # 6. Top grids
    # =====================================================

    print()
    print(
        "6. TOP 10 GRIDS BY TOTAL ACTIVITY"
    )

    cursor.execute(
        """
        SELECT
            g.grid_id,
            SUM(f.total_activity)
                AS total_activity
        FROM fact_network_activity f
        INNER JOIN dim_grid g
            ON f.grid_key = g.grid_key
        GROUP BY
            g.grid_id
        ORDER BY
            total_activity DESC,
            g.grid_id ASC
        LIMIT 10
        """
    )

    top_grids = cursor.fetchall()

    for row in top_grids:

        print(
            f"   Grid {row[0]}: "
            f"{row[1]:,.2f}"
        )

    # =====================================================
    # 7. Hourly trend
    # =====================================================

    print()
    print(
        "7. HOURLY ACTIVITY TREND"
    )

    cursor.execute(
        """
        SELECT
            t.date,
            t.hour,
            SUM(f.total_activity)
                AS total_activity
        FROM fact_network_activity f
        INNER JOIN dim_time t
            ON f.time_key = t.time_key
        GROUP BY
            t.date,
            t.hour
        ORDER BY
            t.date,
            t.hour
        LIMIT 10
        """
    )

    hourly_rows = cursor.fetchall()

    for row in hourly_rows:

        print(
            f"   {row[0]} "
            f"hour={row[1]} "
            f"activity={row[2]:,.2f}"
        )

    # =====================================================
    # 8. Internet-heavy windows
    # =====================================================

    print()
    print(
        "8. INTERNET-HEAVY WINDOWS"
    )

    cursor.execute(
        """
        SELECT
            t.timestamp,
            SUM(
                f.internet_activity
            )
            /
            NULLIF(
                SUM(f.total_activity),
                0
            ) AS internet_share
        FROM fact_network_activity f
        INNER JOIN dim_time t
            ON f.time_key = t.time_key
        GROUP BY
            t.timestamp
        HAVING
            SUM(f.total_activity) > 0
        ORDER BY
            internet_share DESC,
            t.timestamp ASC
        LIMIT 10
        """
    )

    internet_rows = (
        cursor.fetchall()
    )

    for row in internet_rows:

        print(
            f"   {row[0]} "
            f"share={row[1]:.6f}"
        )

    # =====================================================
    # 9. Index validation
    # =====================================================

    cursor.execute(
        """
        SELECT
            table_name,
            index_name,
            GROUP_CONCAT(
                column_name
                ORDER BY seq_in_index
            ) AS columns_used
        FROM information_schema.statistics
        WHERE table_schema = %s
        AND table_name IN (
            'dim_time',
            'dim_grid',
            'fact_network_activity'
        )
        GROUP BY
            table_name,
            index_name
        ORDER BY
            table_name,
            index_name
        """,
        (MYSQL_DATABASE,)
    )

    indexes = cursor.fetchall()

    print()
    print(
        "9. INDEXES"
    )

    for row in indexes:

        print(
            f"   {row[0]} | "
            f"{row[1]} | "
            f"{row[2]}"
        )

    useful_index_names = {
        "idx_dim_time_date",
        "idx_dim_time_hour",
        "idx_dim_grid_grid_id",
        "idx_fact_grid_time",
        "idx_fact_time",
    }

    actual_index_names = {
        row[1]
        for row in indexes
    }

    criterion_9 = (
        useful_index_names
        .issubset(
            actual_index_names
        )
    )

    results[
        "indexes"
    ] = criterion_9

    print(
        "PASS"
        if criterion_9
        else "FAIL"
    )

    # =====================================================
    # Overall
    # =====================================================

    all_passed = all(
        results.values()
    )

    print()
    print("=" * 70)

    print(
        "DE6 VALIDATION: "
        +
        (
            "ALL PASS"
            if all_passed
            else "FAILED"
        )
    )

    print("=" * 70)

    cursor.close()

    connection.close()

    return all_passed


# =========================================================
# Main
# =========================================================

if __name__ == "__main__":

    success = validate()

    raise SystemExit(
        0 if success else 1
    )