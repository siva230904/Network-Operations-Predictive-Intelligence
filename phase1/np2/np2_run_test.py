from usage_processor import UsageProcessor

processor = UsageProcessor(
    file_path="../../data/raw/sms-call-internet-mi-2013-11-01.csv"
)

result = processor.process()


# To run multiple files, you can use the following code snippet:
"""
from pathlib import Path

from usage_processor import UsageProcessor


input_dir = Path("data/landing")
output_dir = Path("data/processed")

files = sorted(
    input_dir.glob("sms-call-internet-mi-*.csv")
)

results = []

for file in files:
    processor = UsageProcessor(file_path=file)

    result = processor.process(
        output_dir=output_dir
    )

    results.append(result)

print(f"Processed {len(results)} files.")\
"""