from difflib import SequenceMatcher

import requests
import spacy
from bs4 import BeautifulSoup


# Function to download the spaCy model if it's not already installed
def download_spacy_model():
    try:
        nlp = spacy.load("en_core_web_sm")
    except OSError:
        print("Downloading the spacy model...")
        from spacy.cli import download

        download("en_core_web_sm")
        nlp = spacy.load("en_core_web_sm")
    return nlp


# Load the spaCy model
nlp = download_spacy_model()


def fetch_data_from_wikipedia(claim):
    """
    Fetches data from Wikipedia based on the claim.
    """
    search_url = f"https://en.wikipedia.org/wiki/{claim.replace(' ', '_')}"
    response = requests.get(search_url)
    if response.status_code == 200:
        print("wikipedia: ", response.text)
        return response.text
    return None


def fetch_data_from_newsapi(claim):
    """
    Fetches data from NewsAPI based on the claim.
    """
    api_key = "c93ba1a06e124304bc293ec7122c5874"  # Replace with your NewsAPI key
    search_url = f"https://newsapi.org/v2/everything?q={claim}&apiKey={api_key}"
    response = requests.get(search_url)
    if response.status_code == 200:
        articles = response.json().get("articles", [])
        print("newsapi: ", articles)

        return " ".join(
            [article["content"] for article in articles if article["content"]]
        )
    return None


def extract_entities(text):
    """
    Extracts named entities from the given text.
    """
    doc = nlp(text)
    entities = [(ent.text, ent.label_) for ent in doc.ents]
    return entities


def analyze_claim(claim):
    """
    Analyzes the credibility of the given claim by fetching and analyzing data.
    """
    # Fetch data from multiple sources
    wikipedia_data = fetch_data_from_wikipedia(claim)
    newsapi_data = fetch_data_from_newsapi(claim)
    combined_data = (wikipedia_data or "") + (newsapi_data or "")

    if not combined_data:
        return {"credibility_score": 0, "message": "No supporting data found"}

    # Parse the data with BeautifulSoup if from Wikipedia
    if wikipedia_data:
        soup = BeautifulSoup(wikipedia_data, "html.parser")
        combined_data += soup.get_text().lower()

    # Extract entities from the claim
    claim_entities = extract_entities(claim)

    # Perform a more advanced NLP analysis
    doc = nlp(combined_data.lower())
    claim_doc = nlp(claim.lower())

    # Enhanced matching logic: semantic similarity
    matches = []
    for sent in doc.sents:
        for ent_text, ent_label in claim_entities:
            if ent_text.lower() in sent.text.lower():
                similarity = SequenceMatcher(
                    None, claim.lower(), sent.text.lower()
                ).ratio()
                if similarity > 0.7:  # Arbitrary similarity threshold
                    matches.append(sent)

    credibility_score = 20 + 60 * (len(matches) > 0)  # Arbitrary scoring for prototype

    return {
        "credibility_score": credibility_score,
        "message": "Analysis complete",
        "matches": [str(sent) for sent in matches],
    }


def main():
    claim = input("Enter the claim to analyze: ")
    result = analyze_claim(claim)
    print(result)


if __name__ == "__main__":
    main()
