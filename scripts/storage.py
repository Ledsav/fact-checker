import firebase_admin
import numpy as np
import pandas as pd
from firebase_admin import credentials, firestore

from scripts.path_operators import get_datasets_dir, get_firebase_key_path


class FirebaseHandler:
    def __init__(self, key_path):
        self.cred = credentials.Certificate(key_path)
        self.app = firebase_admin.initialize_app(self.cred)
        self.db = firestore.client()

    def upsert_data(self, df_instance, collection_name, id_column):
        for index, row in df_instance.iterrows():
            doc_id = row[id_column]
            doc_ref = self.db.collection(collection_name).document(doc_id)

            if not doc_ref.get().exists:  # Check if the document exists
                data = row.to_dict()
                data = self.convert_values(data)
                doc_ref.set(data, merge=True)

    def upsert_grouped_data(self, df_instance, collection_name, id_column):
        for index, row in df_instance.iterrows():
            doc_id = row[id_column].replace(" ", "_").lower()
            doc_ref = self.db.collection(collection_name).document(doc_id)

            data = row.to_dict()
            data = self.convert_values(data)
            doc_ref.set(data, merge=True)

    def convert_values(self, data):
        for key, value in data.items():
            if isinstance(value, np.ndarray):
                data[key] = value.tolist()
            elif pd.isna(value):
                data[key] = None
        return data

    def close(self):
        firebase_admin.delete_app(self.app)


def load_parquet(file_name):
    file_path = get_datasets_dir(file_name)
    return pd.read_parquet(file_path)


def main():
    # Initialize Firebase
    firebase_handler = FirebaseHandler(get_firebase_key_path())

    # Load datasets
    main_df = load_parquet("processed_fact_checking_with_scores.parquet")
    party_df = load_parquet("average_by_party.parquet")
    author_df = load_parquet("average_by_author.parquet")

    # Upsert main dataset
    # firebase_handler.upsert_data(main_df, "fact_checking", "id")

    # Upsert party averages
    # firebase_handler.upsert_grouped_data(party_df, "party_averages", "party")

    # Upsert author averages
    firebase_handler.upsert_grouped_data(author_df, "author_averages", "author")

    # Optionally print for debugging
    print(main_df.head())
    print(party_df.head())
    print(author_df.head())

    # Close Firebase connection
    firebase_handler.close()


if __name__ == "__main__":
    main()
