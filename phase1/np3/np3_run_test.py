from alert_detector import NetworkAlertGenerator

# used first for testing, then for production.  The first file is a subset of the second.
# analytics_file = (
#     "../../data/landing/"
#     "sms-call-internet-mi-2013-11-01_hourly_grid_summary.csv"
# )
analytics_file = (
    "../../data/landing/"
    "hourly_grid_summary.csv"
)

alert_generator = NetworkAlertGenerator(
    file_path=analytics_file,

    # Thresholds — validate and document these.
    high_activity_ratio=2.0,
    spike_ratio=1.5,
    drop_ratio=0.5,

    # Calculate from supplied data.
    activity_floor=None,

    # Dedicated NP3 logs.
    log_dir="../../data/logs"
)

result = alert_generator.process(
    output_dir="../../data/landing",
    filename="network_alerts.csv"
)