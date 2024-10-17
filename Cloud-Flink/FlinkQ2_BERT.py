import pandas as pd
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score
from transformers import DistilBertTokenizer, DistilBertModel
import torch
import numpy as np
from sklearn.preprocessing import StandardScaler
import os  # Import os module to join paths
from glob import glob  # Import glob module to find all CSV files

# Initialize Flink Stream and Table environments
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# Path to your CSV folder
csv_folder_path = "/Users/sarah/Desktop/ccnews-dataset/"

# Use glob to get all CSV files in the folder
csv_files = glob(os.path.join(csv_folder_path, "*.csv"))

# List to hold data from all CSV files
all_data = []

# Loop through each file and read it (without chunking)
for csv_file in csv_files:
    print(f"Processing file: {csv_file}")
    try:
        # Read the entire file without chunking
        df = pd.read_csv(csv_file, low_memory=False, on_bad_lines='skip')
        all_data.append(df)
    except pd.errors.ParserError as e:
        print(f"Error reading {csv_file}: {e}")
        continue

# Concatenate all data into a single DataFrame
df = pd.concat(all_data, ignore_index=True)

# Filter dataset based on language and select necessary columns
df_filtered = df[(df['language'].isin(['ar', 'en']))]
df_selected = df_filtered[['plain_text', 'categories']]

# Handle NaN values in the 'categories' column using .loc
df_selected.loc[:, 'categories'] = df_selected['categories'].fillna('unknown')

# Take a small sample for testing (adjust frac to reduce dataset size further if necessary)
df_sample = df_selected.sample(frac=0.00001, random_state=42)  # Sample 1% for testing

# Use the DistilBERT model and tokenizer
model_name = 'distilbert-base-multilingual-cased'
tokenizer = DistilBertTokenizer.from_pretrained(model_name)
bert_model = DistilBertModel.from_pretrained(model_name)

# Function to tokenize text and get BERT embeddings
def get_bert_embeddings(text):
    try:
        inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=512)
        with torch.no_grad():
            outputs = bert_model(**inputs)
        return outputs.last_hidden_state[:, 0, :].detach().numpy()  # CLS token
    except Exception as e:
        print(f"Error generating embeddings for text: {text}, error: {e}")
        return np.array([])  # Return an empty array in case of failure

# Apply the BERT function to the dataframe and save embeddings to a separate column
df_sample['embeddings'] = df_sample['plain_text'].apply(get_bert_embeddings)

# Check if embeddings are valid
valid_embeddings_count = df_sample['embeddings'].apply(lambda x: x is not None and len(x) > 0).sum()
print(f"{valid_embeddings_count} valid embeddings out of {len(df_sample)}")

# Filter out rows with empty embeddings
df_sample = df_sample[df_sample['embeddings'].apply(lambda x: x is not None and len(x) > 0)]

# Convert the list of embeddings to a NumPy array for model training
if len(df_sample) > 0:  # Ensure there's data to train on
    X = np.vstack(df_sample['embeddings'].values)
    y = df_sample['categories'].values
    # Split the dataset into training and testing
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    # Normalize the embeddings using StandardScaler
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    # Initialize Logistic Regression model with more iterations
    classifier = LogisticRegression(max_iter=500)
    # Train the classifier
    classifier.fit(X_train_scaled, y_train)
    # Make predictions
    y_pred = classifier.predict(X_test_scaled)
    # Evaluate accuracy and precision
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, average='weighted', zero_division=1)

    print(f"Accuracy: {accuracy}")
    print(f"Precision: {precision}")
    # Save the results back to a new CSV with the predicted categories
    df_sample['predicted_category'] = classifier.predict(X)
    df_sample.to_csv('predicted_articles.csv', index=False)
else:
    print("No valid data available for training.")
