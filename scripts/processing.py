import re
from datetime import datetime

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

MONTHS_IT = {
    "gennaio": "January",
    "febbraio": "February",
    "marzo": "March",
    "aprile": "April",
    "maggio": "May",
    "giugno": "June",
    "luglio": "July",
    "agosto": "August",
    "settembre": "September",
    "ottobre": "October",
    "novembre": "November",
    "dicembre": "December",
    "GENNAIO": "January",
    "FEBBRAIO": "February",
    "MARZO": "March",
    "APRILE": "April",
    "MAGGIO": "May",
    "GIUGNO": "June",
    "LUGLIO": "July",
    "AGOSTO": "August",
    "SETTEMBRE": "September",
    "OTTOBRE": "October",
    "NOVEMBRE": "November",
    "DICEMBRE": "December",
}


def load_dataset(file_path):
    return pd.read_parquet(file_path)


def classify_verdict(verdict):
    verdict = verdict.lower()
    if any(re.search(rf"\b{kw}\b", verdict) for kw in NEGATIVE_KEYWORDS):
        return -1
    elif any(re.search(rf"\b{kw}\b", verdict) for kw in NEUTRAL_KEYWORDS):
        return 0
    elif any(re.search(rf"\b{kw}\b", verdict) for kw in POSITIVE_KEYWORDS):
        return 1
    else:
        return 0  # Default to neutral if no keywords are found


def standardize_date(date_str):
    if pd.isna(date_str) or date_str.strip() == "":
        return "1900-01-01"  # Assign a default old date for missing or empty dates

    # Replace Italian months with English months
    for it_month, en_month in MONTHS_IT.items():
        date_str = date_str.replace(it_month, en_month)

    # Try parsing different date formats
    for date_format in (
        "%d %B %Y",
        "%d %b %Y",
        "%B %Y",
        "%b %Y",
        "%d %B",
        "%d %b",
        "%B",
        "%b",
    ):
        try:
            parsed_date = datetime.strptime(date_str, date_format)
            # If year is missing, add default year
            if parsed_date.year == 1900:
                parsed_date = parsed_date.replace(year=1900)
            # If month is missing, add default month
            if (
                parsed_date.month == 1
                and "%B" not in date_format
                and "%b" not in date_format
            ):
                parsed_date = parsed_date.replace(month=1)
            # If day is missing, add default day
            if parsed_date.day == 1 and "%d" not in date_format:
                parsed_date = parsed_date.replace(day=1)
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return "1900-01-01"  # Return default old date if no format matches


def process_dataset(df):
    df = df[df["verdict"] != "No verdict available"]
    df = df.dropna(
        subset=["verdict", "author", "party"]
    )  # Remove rows with NaN in 'verdict', 'author', or 'party'
    df = df[df["author"].str.strip() != ""]  # Remove rows with empty author
    df = df[df["party"].str.strip() != ""]  # Remove rows with empty party
    df = df.drop_duplicates(subset=["id"])
    df["date"] = df["date"].apply(standardize_date)
    df["score"] = df["verdict"].apply(classify_verdict)
    df = df.sort_values(by="date", ascending=False)
    return df


def save_dataset(df, file_path):
    df.to_parquet(file_path, index=False, engine="pyarrow")


def create_grouped_parquets(df):
    # Group by party and calculate mean score and count
    df_party = (
        df.groupby("party")
        .agg(average_score=("score", "mean"), count=("score", "size"))
        .reset_index()
    )

    # Group by author and calculate mean score and count
    df_author = (
        df.groupby("author")
        .agg(average_score=("score", "mean"), count=("score", "size"))
        .reset_index()
    )

    save_dataset(df_party, get_datasets_dir("average_by_party.parquet"))
    save_dataset(df_author, get_datasets_dir("average_by_author.parquet"))


def main():
    # Main function to run the script
    input_path = get_datasets_dir("fact_checking_with_verdict.parquet")
    output_path = get_datasets_dir("processed_fact_checking_with_scores.parquet")

    # Load the dataset
    df = load_dataset(input_path)

    # Process the dataset
    df = process_dataset(df)

    # Save the processed dataset
    save_dataset(df, output_path)

    # Create additional parquet files
    create_grouped_parquets(df)

    # Print the DataFrame (optional for debugging)
    print(df)


if __name__ == "__main__":
    main()
