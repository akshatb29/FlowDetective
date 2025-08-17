# train_model_rf.py

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import joblib
import random
from datetime import datetime, timedelta

# Define attack types
attack_types = ["Normal", "DDoS", "Malware", "Phishing", "Zero-Day Exploit"]

n_samples = 1000  # total samples
data = []

for _ in range(n_samples):
    attack = random.choice(attack_types)
    
    # Generate features based on attack type
    if attack == "Normal":
        bytes_sent = np.random.normal(1000, 200)
    elif attack == "DDoS":
        bytes_sent = np.random.normal(8000, 1000)
    elif attack == "Malware":
        bytes_sent = np.random.normal(3000, 500)
    elif attack == "Phishing":
        bytes_sent = np.random.normal(500, 100)
    else:  # Zero-Day Exploit
        bytes_sent = np.random.normal(10000, 1500)
    
    # Random IPs
    src_ip_int = np.random.randint(1, 255_255_255)
    dst_ip_int = np.random.randint(1, 255_255_255)
    
    # Timestamp features
    hour_of_day = np.random.randint(0, 24)
    
    # Optional derived feature
    ip_diff = abs(src_ip_int - dst_ip_int)
    
    data.append([bytes_sent, src_ip_int, dst_ip_int, hour_of_day, ip_diff, attack])

# Create DataFrame
df = pd.DataFrame(data, columns=['bytes_sent', 'src_ip_int', 'dst_ip_int', 'hour_of_day', 'ip_diff', 'label'])

# Split features and labels
X = df[['bytes_sent', 'src_ip_int', 'dst_ip_int', 'hour_of_day', 'ip_diff']]
y = df['label']

# Initialize Random Forest Classifier
rf_model = RandomForestClassifier(
    n_estimators=200,     # number of trees
    max_depth=10,         # limit depth to prevent overfitting
    random_state=42
)

# Train the model
rf_model.fit(X, y)

# Save the trained model
joblib.dump(rf_model, 'rf_model.pkl')
print("Random Forest model saved as rf_model.pkl")

