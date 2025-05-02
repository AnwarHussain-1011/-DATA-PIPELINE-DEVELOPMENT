# -DATA-PIPELINE-DEVELOPMENT

import pandas as pd
from sklearn.preprocessing import StandardScaler
import os

# Step 0: Create sample data directory and file
def create_sample_data():
    os.makedirs("data", exist_ok=True)
    sample_data = {
        "Name": ["John", "Jane", "Doe", "Alex"],
        "Age": [29, 31, None, 28],
        "Salary": [50000, 60000, 55000, 58000]
    }
    df = pd.DataFrame(sample_data)
    df.to_csv("data/sample_data.csv", index=False)
    print("✅ Sample data created at 'data/sample_data.csv'")

# Step 1: Extract
def extract_data(file_path):
    df = pd.read_csv(file_path)
    print("\n📥 Data Extracted:")
    print(df.head())
    return df

# Step 2: Transform
def transform_data(df):
    df_cleaned = df.dropna()
    numeric_cols = df_cleaned.select_dtypes(include='number').columns
    scaler = StandardScaler()
    df_cleaned[numeric_cols] = scaler.fit_transform(df_cleaned[numeric_cols])
    print("\n🔧 Data Transformed:")
    print(df_cleaned.head())
    return df_cleaned

# Step 3: Load
def load_data(df, output_path):
    df.to_csv(output_path, index=False)
    print(f"\n📤 Transformed data saved to: {output_path}")

# Run the ETL pipeline
def run_pipeline():
    create_sample_data()
    input_path = "data/sample_data.csv"
    output_path = "data/processed_data.csv"
    
    df = extract_data(input_path)
    df_transformed = transform_data(df)
    load_data(df_transformed, output_path)

if __name__ == "__main__":
    run_pipeline()
