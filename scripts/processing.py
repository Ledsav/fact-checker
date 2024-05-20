import re

import pandas as pd

from scripts.path_operators import get_datasets_dir

# Define the keywords and phrases for classification
NEGATIVE_KEYWORDS = [
    "falsa",
    "scorretta",
    "sbagliata",
    "non è supportata",
    "non stanno proprio",
    "esagerata",
    "imprecisa",
    "parecchio esagerato",
    "fuorviante",
    "omette",
    "non la racconta giusta",
    "fa confusione",
    "smentiscono",
    "danno torto",
    "confonde",
    "completamente sbagliato",
    "sbaglia",
    "torto",
    "dà meriti che non ha",
]
NEUTRAL_KEYWORDS = [
    "esagera",
    "troppo semplice",
    "parziale",
    "troppo ottimista",
    "semplificazione",
    "eccessivo",
    "dipende",
    "provvisori",
]
POSITIVE_KEYWORDS = [
    "verità",
    "corretta",
    "corrette",
    "più alte",
    "Sì",
    "ha ragione",
    "sostanzialmente corretta",
    "danno ragione",
    "sono corretti",
    "supportata dai fatti",
    "attendibile",
    "previsioni scientifiche",
    "ordine di grandezza è corretto",
    "cifre esatte",
]


def load_dataset(file_path):
    # Function to load the dataset from a Parquet file
    return pd.read_parquet(file_path)


def classify_verdict(verdict):
    # Function to classify the verdict based on keywords
    verdict = verdict.lower()
    if any(re.search(rf"\b{kw}\b", verdict) for kw in NEGATIVE_KEYWORDS):
        return -1
    elif any(re.search(rf"\b{kw}\b", verdict) for kw in NEUTRAL_KEYWORDS):
        return 0
    elif any(re.search(rf"\b{kw}\b", verdict) for kw in POSITIVE_KEYWORDS):
        return 1
    else:
        return 0  # Default to neutral if no keywords are found


def process_dataset(df):
    # Function to process the dataset and classify verdicts
    df["score"] = df["verdict"].apply(classify_verdict)
    return df


def save_dataset(df, file_path):
    # Function to save the processed dataset to a Parquet file
    df.to_parquet(file_path, index=False, engine="pyarrow")


def main():
    # Main function to run the script
    input_path = get_datasets_dir() / "fact_checking_with_verdict.parquet"
    output_path = get_datasets_dir() / "processed_fact_checking_with_scores.parquet"

    # Load the dataset
    df = load_dataset(input_path)

    # Process the dataset
    df = process_dataset(df)

    # Save the processed dataset
    save_dataset(df, output_path)

    # Print the DataFrame
    print(df)


if __name__ == "__main__":
    main()
