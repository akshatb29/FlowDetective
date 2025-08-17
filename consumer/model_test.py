import joblib
import numpy as np

model = joblib.load('rf_model.pkl')

# Test with a normal point
# Test on a sample
sample= np.array([[5000, 192_168_0_12, 10_0_0_8, 14, 12_168_0_12- 10_0_0_8]])
pred = model.predict(sample)
pred_proba = model.predict_proba(sample)
print("Sample prediction:", pred)
print("Class probabilities:", pred_proba)

