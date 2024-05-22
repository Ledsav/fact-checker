from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns

from scripts.path_operators import get_datasets_dir


# Load the dataset
def load_dataset(file_path):
    """
    Load the dataset from a Parquet file.

    :param file_path: Path to the Parquet file
    :return: DataFrame with the loaded data
    """
    if not Path(file_path).exists():
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
        f"{lowest_avg_score['normalized_score']:.2f}"
    )


# Find party with highest score
def find_highest_avg_score_by_party(df):
    credibility_by_party = get_score_df_grouped_by_party(df)
    highest_avg_score = credibility_by_party.loc[
        credibility_by_party["normalized_score"].idxmax()
    ]
    print(
        f"The party with the highest average credibility score is: {highest_avg_score['party']} with a score of "
        f"{highest_avg_score['normalized_score']:.2f}"
    )


# Plot politician credibility
def plot_credibility_by_politician(df):
    credibility_by_politician = get_normalized_score_df_grouped_by_politician(df)
    credibility_by_politician = credibility_by_politician.sort_values(
        by="normalized_score", ascending=False
    )
    plt.figure(figsize=(14, 8))
    sns.set_theme(style="darkgrid")
    sns.barplot(
        x="normalized_score",
        y="author",
        hue="author",
        data=credibility_by_politician,
        palette="viridis",
        dodge=False,
        legend=False,
    )
    plt.xlabel("Normalized Average Credibility Score")
    plt.ylabel("Politician")
    plt.title("Overall Credibility by Politicians")
    plt.tight_layout()
    plt.show()


# Plot party credibility
def plot_credibility_by_party(df):
    credibility_by_party = get_score_df_grouped_by_party(df)
    credibility_by_party = credibility_by_party.sort_values(
        "normalized_score", ascending=False
    )
    plt.figure(figsize=(14, 8))
    sns.set_theme(style="darkgrid")
    sns.barplot(
        x="normalized_score",
        y="party",
        hue="party",
        data=credibility_by_party,
        palette="viridis",
        dodge=False,
        legend=False,
    )
    plt.xlabel("Normalized Average Credibility Score")
    plt.ylabel("Party")
    plt.title("Overall Credibility by Parties")
    plt.tight_layout()
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
    plt.legend(loc="upper right", bbox_to_anchor=(1.15, 1))
    plt.tight_layout()
    plt.show()


def plot_interactive(df, mode="light"):
    # Calculate average scores for each politician
    avg_scores = df.groupby("author")["score"].mean().reset_index()

    # Settings for dark or light mode
    if mode == "dark":
        background_color = "#1f1f1f"
        text_color = "#ffffff"
        grid_color = "#444444"
        template = "plotly_dark"
    else:
        background_color = "#ffffff"
        text_color = "#000000"
        grid_color = "#cccccc"
        template = "plotly_white"

    fig = go.Figure()

    for i, author in enumerate(avg_scores["author"].unique()):
        author_data = avg_scores[avg_scores["author"] == author]
        fig.add_trace(
            go.Bar(
                x=author_data["score"],
                y=author_data["author"],
                name=author,
                orientation="h",
                marker=dict(
                    line=dict(color="#000000", width=1),
                    opacity=0.8,
                ),
                hoverinfo="x+y+name",
                hoverlabel=dict(bgcolor="#ffffff", font_size=16, font_family="Roboto"),
            )
        )

    fig.update_layout(
        title={
            "text": "Interactive Average Scores by Politicians",
            "y": 0.95,
            "x": 0.5,
            "xanchor": "center",
            "yanchor": "top",
        },
        title_font=dict(size=24, color=text_color),
        xaxis=dict(
            title="Average Score",
            titlefont=dict(size=18, color=text_color),
            tickfont=dict(size=14, color=text_color),
            showgrid=False,  # Hide the grid for a cleaner look
            zeroline=False,  # Hide the zero line
        ),
        yaxis=dict(
            title="Politician",
            titlefont=dict(size=18, color=text_color),
            tickfont=dict(size=14, color=text_color),
            showgrid=True,
            gridcolor=grid_color,
        ),
        paper_bgcolor=background_color,
        plot_bgcolor=background_color,
        font=dict(color=text_color, family="Roboto"),
        margin=dict(l=100, r=50, t=100, b=50),
        hovermode="y unified",
        template=template,
    )

    fig.show()


def main():
    df = load_dataset(get_datasets_dir("processed_fact_checking_with_scores.parquet"))

    # Find the politician with the lowest average credibility score
    find_lowest_avg_score_by_politician(df)

    # Plot the average credibility scores for politicians
    plot_credibility_by_politician(df)

    # Find the party with the highest average credibility score
    find_highest_avg_score_by_party(df)

    # Plot the average credibility scores for parties
    plot_credibility_by_party(df)

    # Plot interactive average scores
    plot_interactive(df)


if __name__ == "__main__":
    main()
