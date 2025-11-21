import sagemaker
import boto3
import pandas as pd
import numpy as np
from sagemaker import get_execution_role
from sagemaker.inputs import TrainingInput
from sagemaker.xgboost.estimator import XGBoost
from sklearn.model_selection import train_test_split

# Configuration
role = get_execution_role()
session = sagemaker.Session()
region = boto3.Session().region_name
bucket = "c732-sfu-parking-data-lake" 
prefix = "sagemaker/sfu-parking"

# input path 
training_data_s3 = f"s3://{bucket}/processed/training_features/"

print(f"Region: {region}")
print(f"Role: {role}")
print(f"Reading data from: {training_data_s3}")

# load data
df = pd.read_parquet(training_data_s3)
print(f"Data Loaded. Shape: {df.shape}")

# drop non-feature columns for training (keep lot_id/campus but encode them)
# One-Hot Encode categorical variables
df_encoded = pd.get_dummies(df, columns=['lot_id', 'campus'])

# Define features (X) and labels (Y)
# drop target columns and identifiers not needed for X
features = df_encoded.drop(['occupancy_plus_15m', 'departures_in_next_15m', 'timestamp', 'date'], axis=1, errors='ignore')

# targets
label_occupancy = df['occupancy_plus_15m']
label_departure = df['departures_in_15m']

# split Data (80% training, 20% validation)
X_train, X_val, y_occ_train, y_occ_val, y_dep_train, y_dep_val = train_test_split(
    features, label_occupancy, label_departure, test_size=0.2, random_state=42
)

# Model training using XGBOOST 
# XGBoost in SageMaker expects CSV data in S3 with no headers
# first column = target label, remaining columns = features
def upload_to_s3(x, y, name):
    # combine label + features
    dataset = pd.concat([y, x], axis=1)
    filename = f"{name}.csv"
    dataset.to_csv(filename, header=False, index=False)
    
    return session.upload_data(filename, bucket=bucket, key_prefix=f"{prefix}/input/{name}")

# upload occupancy data
train_occ_uri = upload_to_s3(X_train, y_occ_train, 'train_occupancy')
val_occ_uri = upload_to_s3(X_val, y_occ_val, 'val_occupancy')

# upload departure data
train_dep_uri = upload_to_s3(X_train, y_dep_train, 'train_departure')
val_dep_uri = upload_to_s3(X_val, y_dep_val, 'val_departure')

print("Data uploaded to S3 for training.")

# Training 2 models
# Retrieve XGBoost container image
xgboost_container = sagemaker.image_uris.retrieve("xgboost", region, "1.5-1")

hyperparams = {
    "max_depth": "5",
    "eta": "0.2",
    "gamma": "4",
    "min_child_weight": "6",
    "subsample": "0.8",
    "objective": "reg:squarederror",
    "num_round": "100"
}

# Train model A: occupancy predictor 
print("Training Occupancy Model...")
xgb_occ = sagemaker.estimator.Estimator(
    image_uri=xgboost_container,
    role=role,
    instance_count=1,
    instance_type="ml.m5.large",
    output_path=f"s3://{bucket}/{prefix}/output/occupancy",
    sagemaker_session=session
)
xgb_occ.set_hyperparameters(**hyperparams)
xgb_occ.fit({'train': TrainingInput(train_occ_uri, content_type='csv'), 
             'validation': TrainingInput(val_occ_uri, content_type='csv')})

# Train model B: departure predictor
print("Training Departure Model...")
xgb_dep = sagemaker.estimator.Estimator(
    image_uri=xgboost_container,
    role=role,
    instance_count=1,
    instance_type="ml.m5.large",
    output_path=f"s3://{bucket}/{prefix}/output/departure",
    sagemaker_session=session
)
xgb_dep.set_hyperparameters(**hyperparams)
xgb_dep.fit({'train': TrainingInput(train_dep_uri, content_type='csv'), 
             'validation': TrainingInput(val_dep_uri, content_type='csv')})

print("Deploying Endpoints...")

# Deploy occupancy model
predictor_occ = xgb_occ.deploy(
    initial_instance_count=1,
    instance_type="ml.t2.medium", 
    endpoint_name="sfu-occupancy-predictor"
)

# Deploy departure model
predictor_dep = xgb_dep.deploy(
    initial_instance_count=1,
    instance_type="ml.t2.medium",
    endpoint_name="sfu-departure-predictor"
)

print("\n--- DEPLOYMENT COMPLETE ---")
print("Endpoint 1: sfu-occupancy-predictor")
print("Endpoint 2: sfu-departure-predictor")