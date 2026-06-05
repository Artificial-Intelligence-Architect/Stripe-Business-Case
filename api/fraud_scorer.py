import joblib
import numpy as np

# Load a pre‑trained model (you must provide one)
# If you don't have one, we return a random score for demonstration purposes.
try:
    model = joblib.load("models/fraud_model.pkl")
    MODEL_AVAILABLE = True
except:
    MODEL_AVAILABLE = False
    print("No model found, using random scoring fallback.")

def predict_score(features: dict) -> float:
    if MODEL_AVAILABLE:
        # features must be a dict with the same keys as the training data
        # Simplified example
        X = np.array([list(features.values())])
        score = model.predict_proba(X)[0][1]
        return float(score)
    else:
        # Fallback: random score between 0 and 1
        return np.random.random()