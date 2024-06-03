import firebase_admin
import pandas as pd
from firebase_admin import credentials, firestore

from scripts.path_operators import get_datasets_dir, get_firebase_key_path

# Path to your Parquet file
file_path = get_datasets_dir("processed_fact_checking_with_scores.parquet")

# Read the Parquet file
df = pd.read_parquet(file_path)

# Path to your Firebase service account key file
cred = credentials.Certificate(get_firebase_key_path())

# Initialize the app with a service account, granting admin privileges
app = firebase_admin.initialize_app(cred)

# Initialize Firestore
db = firestore.client()


def upsert_data(df, db):
    # Iterate through the DataFrame and upsert each row
    for index, row in df.iterrows():
        doc_id = row["id"]  # Replace 'your_id_column' with the actual column name
        doc_ref = db.collection("fact_checking").document(doc_id)

        # Convert the row to a dictionary
        data = row.to_dict()

        # Upsert the document
        doc_ref.set(data, merge=True)


# Call the upsert function
upsert_data(df, db)

# Optional: Revoke the token or cleanup if necessary
firebase_admin.delete_app(app)
