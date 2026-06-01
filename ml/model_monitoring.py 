"""
Model Monitoring - Drift Detection (Evidently AI)
Detects data drift and performance variations.
"""

from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, ClassificationPreset
import pandas as pd

def check_model_health(reference_data: pd.DataFrame,
                       current_data: pd.DataFrame) -> dict:
    """
    Checks for data drift and model performance.
    Returns a dictionary containing metrics and a retraining flag.
    """
    report = Report(metrics=[
        DataDriftPreset(),
        ClassificationPreset()
    ])

    report.run(
        reference_data=reference_data,
        current_data=current_data
    )

    result = report.as_dict()

    # Retraining trigger thresholds
    auc_threshold = 0.92
    drift_threshold = 0.15

    current_auc = result["metrics"][1]["result"]["current"]["roc_auc"]
    drift_score = result["metrics"][0]["result"]["drift_share"]

    needs_retraining = (
        current_auc < auc_threshold or
        drift_score > drift_threshold
    )

    return {
        "needs_retraining": needs_retraining,
        "current_auc": current_auc,
        "drift_score": drift_score,
        "report": result
    }


# Local usage example
if __name__ == "__main__":
    # Reference data (training)
    reference = pd.DataFrame({
        "feature1": [0.1, 0.2, 0.3],
        "feature2": [10, 20, 30],
        "target": [0, 1, 0]
    })

    # Current data (production)
    current = pd.DataFrame({
        "feature1": [0.2, 0.25, 0.4],
        "feature2": [12, 25, 35],
        "target": [0, 1, 1]
    })

    result = check_model_health(reference, current)
    print(f"Retraining needed: {result['needs_retraining']}")
    print(f"Current AUC: {result['current_auc']}, Drift: {result['drift_score']}")