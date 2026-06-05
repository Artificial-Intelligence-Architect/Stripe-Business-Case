"""
Drift detection for fraud detection features.
Compares current day features with training reference.
"""
import pandas as pd
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_drift_detection(ref_path: str, current_path: str, output_path: str = "drift_report.html"):
    ref = pd.read_csv(ref_path)
    current = pd.read_csv(current_path)
    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=ref, current_data=current)
    report.save_html(output_path)
    logger.info(f"Drift report saved to {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ref", required=True, help="Path to reference features (training)")
    parser.add_argument("--current", required=True, help="Path to current features")
    parser.add_argument("--output", default="drift_report.html")
    args = parser.parse_args()
    run_drift_detection(args.ref, args.current, args.output)