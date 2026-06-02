"""
Generates an Evidently data drift report between two datasets
and saves it to demo/screenshots/evidently_report.html.
"""
import pandas as pd
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
import os

# 1. Create reference data (training)
reference = pd.DataFrame({
    "amount": [50, 120, 80, 200, 35],
    "country_code": ["US", "FR", "US", "DE", "GB"],
    "hour": [10, 14, 22, 3, 18],
    "target": [0, 0, 0, 1, 0]
})

# 2. Current data (production) – simulated with a slight drift
current = pd.DataFrame({
    "amount": [55, 130, 75, 250, 40, 60],
    "country_code": ["US", "FR", "US", "IT", "GB", "US"],
    "hour": [11, 15, 22, 4, 19, 12],
    "target": [0, 1, 0, 1, 0, 0]
})

# 3. Generate the report
report = Report(metrics=[DataDriftPreset()])
report.run(reference_data=reference, current_data=current)

# 4. Save the report
output_dir = "demo/screenshots"
os.makedirs(output_dir, exist_ok=True)
report.save_html(os.path.join(output_dir, "evidently_report.html"))
print(f"Report saved to {output_dir}/evidently_report.html")