import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from scripts.path_operators import get_datasets_dir


# Load the dataset
def load_dataset(file_path):
    """
    Load the dataset from a Parquet file.

    :param file_path: Path to the Parquet file
    :return: DataFrame with the loaded data
    """
    if not file_path.exists():
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    return pd.read_parquet(file_path)


# Normalize score
def get_max_count(grouped):
    max_count = grouped["count"].max()
    grouped["normalized_score"] = grouped.apply(
        lambda row: row["mean"] * (row["count"] / max_count)
        + row["mean"] * (1 - row["count"] / max_count),
        axis=1,
    )
    return grouped


# Group by politician
def get_normalized_score_df_grouped_by_politician(df):
    grouped = df.groupby("author")["score"].agg(["mean", "count"]).reset_index()
    return get_max_count(grouped)


# Group by party
def get_score_df_grouped_by_party(df):
    grouped = df.groupby("party")["score"].agg(["mean", "count"]).reset_index()
    return get_max_count(grouped)


# Find politician with lowest score
def find_lowest_avg_score_by_politician(df):
    credibility_by_politician = get_normalized_score_df_grouped_by_politician(df)
    lowest_avg_score = credibility_by_politician.loc[
        credibility_by_politician["normalized_score"].idxmin()
    ]
    print(
        f"The politician with the lowest average credibility score is: {lowest_avg_score['author']} with a score of "
        f"{lowest_avg_score['normalized_score']}"
    )


# Find party with highest score
def find_highest_avg_score_by_party(df):
    credibility_by_party = get_score_df_grouped_by_party(df)
    highest_avg_score = credibility_by_party.loc[
        credibility_by_party["normalized_score"].idxmax()
    ]
    print(
        f"The party with the highest average credibility score is: {highest_avg_score['party']} with a score of "
        f"{highest_avg_score['normalized_score']}"
    )


# Plot politician credibility
def plot_credibility_by_politician(df):
    credibility_by_politician = get_normalized_score_df_grouped_by_politician(df)
    credibility_by_politician = credibility_by_politician.sort_values(
        by="normalized_score", ascending=False
    )
    top_politicians = credibility_by_politician.head(10)
    bottom_politicians = credibility_by_politician.tail(10)
    selected_politicians = pd.concat([top_politicians, bottom_politicians])

    plt.figure(figsize=(14, 8))
    sns.set_theme(style="darkgrid")
    sns.barplot(
        x="normalized_score",
        y="author",
        data=selected_politicians,
        palette="viridis",
        hue="author",
        dodge=False,
        legend=False,
    )
    plt.xlabel("Normalized Average Credibility Score")
    plt.ylabel("Politician")
    plt.title("Overall Credibility by Politicians")
    plt.show()


# Plot party credibility
def plot_credibility_by_party(df):
    credibility_by_party = get_score_df_grouped_by_party(df)
    credibility_by_party = credibility_by_party.sort_values(
        by="normalized_score", ascending=False
    )
    top_parties = credibility_by_party.head(10)
    bottom_parties = credibility_by_party.tail(10)
    selected_parties = pd.concat([top_parties, bottom_parties])

    plt.figure(figsize=(14, 8))
    sns.set_theme(style="darkgrid")
    sns.barplot(
        x="normalized_score",
        y="party",
        data=selected_parties,
        palette="viridis",
        hue="party",
        dodge=False,
        legend=False,
    )
    plt.xlabel("Normalized Average Credibility Score")
    plt.ylabel("Party")
    plt.title("Overall Credibility by Parties")
    plt.show()


# Additional visualizations


def plot_score_distribution(df):
    plt.figure(figsize=(14, 8))
    sns.set_theme(style="darkgrid")
    sns.histplot(
        data=df, x="score", hue="author", multiple="stack", bins=10, palette="viridis"
    )
    plt.xlabel("Score")
    plt.ylabel("Count")
    plt.title("Distribution of Scores by Politicians")
    plt.legend(loc="upper right")
    plt.show()


def plot_scores_over_time(df):
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    plt.figure(figsize=(14, 8))
    sns.set_theme(style="darkgrid")
    sns.lineplot(
        data=df, x="date", y="score", hue="author", palette="viridis", marker="o"
    )
    plt.xlabel("Date")
    plt.ylabel("Score")
    plt.title("Scores Over Time by Politicians")
    plt.legend(loc="upper right")
    plt.show()


def main():
    df = load_dataset(
        get_datasets_dir() / "processed_fact_checking_with_scores.parquet"
    )

    # Find the politician with the lowest average credibility score
    find_lowest_avg_score_by_politician(df)

    # Plot the average credibility scores for politicians
    plot_credibility_by_politician(df)

    # Find the party with the highest average credibility score
    find_highest_avg_score_by_party(df)

    # Plot the average credibility scores for parties
    plot_credibility_by_party(df)

    # Additional visualizations
    plot_score_distribution(df)
    plot_scores_over_time(df)


if __name__ == "__main__":
    main()
