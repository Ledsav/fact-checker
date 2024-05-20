import string
import time
from concurrent.futures import ThreadPoolExecutor
from difflib import SequenceMatcher

import pandas as pd
import requests
import spacy
from bs4 import BeautifulSoup
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from tabulate import tabulate

import config


class APIRateLimiter:
    def __init__(self, max_calls_per_minute):
        self.max_calls_per_minute = max_calls_per_minute
        self.call_times = []

    def can_make_call(self):
        current_time = time.time()
        self.call_times = [t for t in self.call_times if t > current_time - 60]
        return len(self.call_times) < self.max_calls_per_minute

    def record_call(self):
        self.call_times.append(time.time())


class DataFetcher:
    def __init__(self):
        self.rate_limiter = APIRateLimiter(max_calls_per_minute=10)
        self.nlp = self._load_spacy_model()

    def _load_spacy_model(self):
        try:
            nlp = spacy.load("en_core_web_sm")
        except OSError:
            print("Downloading the spacy model...")
            from spacy.cli import download

            download("en_core_web_sm")
            nlp = spacy.load("en_core_web_sm")
        return nlp

    def fetch_data_from_google_fact_check(self, claim):
        api_key = config.GOOGLE_FACT_CHECK_API_KEY
        search_url = f"https://factchecktools.googleapis.com/v1alpha1/claims:search?query={claim}&key={api_key}"

        print(f"Making request to URL: {search_url}")  # Debugging information
        if not self.rate_limiter.can_make_call():
            print("Rate limit exceeded, waiting...")
            time.sleep(60)  # Wait for a minute before retrying
            if not self.rate_limiter.can_make_call():
                return None

        response = requests.get(search_url)
        if response.status_code == 200:
            self.rate_limiter.record_call()
            claims = response.json().get("claims", [])
            return claims
        return None

    def fetch_data_from_wikipedia(self, claim):
        search_url = f"https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch={claim}&utf8=&format=json"
        response = requests.get(search_url)
        if response.status_code == 200:
            search_results = response.json().get("query", {}).get("search", [])
            if search_results:
                page_title = search_results[0]["title"]
                page_url = (
                    f"https://en.wikipedia.org/wiki/{page_title.replace(' ', '_')}"
                )
                page_response = requests.get(page_url)
                if page_response.status_code == 200:
                    return page_response.text
        return None

    def fetch_data_from_newsapi(self, claim):
        api_key = config.NEWSAPI_KEY
        search_url = f"https://newsapi.org/v2/everything?q={claim}&apiKey={api_key}"
        response = requests.get(search_url)
        if response.status_code == 200:
            articles = response.json().get("articles", [])
            return " ".join(
                [article["content"] for article in articles if article["content"]]
            )
        return None

    def extract_entities(self, text):
        doc = self.nlp(text)
        entities = [(ent.text, ent.label_) for ent in doc.ents]
        return entities


class ClaimStandardizer:

    def __init__(self, claim, language="english"):
        self.claim = claim
        self.language = language

    def standardize(self) -> str:
        standardized_claim = self.claim.strip()
        standardized_claim = standardized_claim.lower()

        standardized_claim = standardized_claim.translate(
            str.maketrans("", "", string.punctuation)
        )

        tokens = word_tokenize(standardized_claim)
        stop_words = set(stopwords.words(self.language))
        filtered_tokens = [token for token in tokens if token not in stop_words]
        optimized_claim = " ".join(filtered_tokens)

        return optimized_claim


class ClaimAnalyzer:
    def __init__(self, services):
        self.data_fetcher = DataFetcher()
        self.claim_standardizer = ClaimStandardizer
        self.services = services

    def analyze_claim(self, claim):
        futures = {}
        claim = self.claim_standardizer(claim).standardize()
        print("Standardized claim: ", claim)

        with ThreadPoolExecutor() as executor:
            if "wikipedia" in self.services:
                futures["wikipedia"] = executor.submit(
                    self.data_fetcher.fetch_data_from_wikipedia, claim
                )
            if "newsapi" in self.services:
                futures["newsapi"] = executor.submit(
                    self.data_fetcher.fetch_data_from_newsapi, claim
                )
            if "google_fact_check" in self.services:
                futures["google_fact_check"] = executor.submit(
                    self.data_fetcher.fetch_data_from_google_fact_check, claim
                )

            results = {service: future.result() for service, future in futures.items()}

        combined_data = ""
        if "wikipedia" in results:
            combined_data += results["wikipedia"] or ""
            if results["wikipedia"]:
                soup = BeautifulSoup(results["wikipedia"], "html.parser")
                combined_data += soup.get_text().lower()

        if "newsapi" in results:
            combined_data += results["newsapi"] or ""

        claim_entities = self.data_fetcher.extract_entities(claim)
        wiki_news_matches = self._find_matches(claim, claim_entities, combined_data)

        google_fact_matches = []
        if "google_fact_check" in results and results["google_fact_check"]:
            google_fact_matches = [
                fact
                for fact in results["google_fact_check"]
                if fact.get("text") and fact["text"].lower() in combined_data.lower()
            ]

        return {
            "message": "Analysis complete",
            "wikipedia_data": results.get("wikipedia"),
            "newsapi_data": results.get("newsapi"),
            "google_fact_check_data": results.get("google_fact_check"),
            "wiki_news_matches": wiki_news_matches,
            "google_fact_matches": google_fact_matches,
        }

    def _find_matches(self, claim, claim_entities, combined_data):
        doc = self.data_fetcher.nlp(combined_data.lower())
        matches = []
        for sent in doc.sents:
            for ent_text, ent_label in claim_entities:
                if ent_text.lower() in sent.text.lower():
                    similarity = SequenceMatcher(
                        None, claim.lower(), sent.text.lower()
                    ).ratio()
                    if similarity > 0.7:
                        matches.append(sent)
        return matches


def get_sentiment_score(text):
    sid = SentimentIntensityAnalyzer()
    sentiment_scores = sid.polarity_scores(text)
    compound_score = sentiment_scores["compound"]
    score = (compound_score + 1) / 2 * 100  # Scale from [-1, 1] to [0, 100]
    print(f"compound_score: {compound_score}, score: {score}")
    return score


def format_google_fact_check_results(fact_check_data):
    if not fact_check_data:
        print("No Google Fact Check data found.")
        return

    formatted_data = []
    for claim in fact_check_data:
        text = claim.get("text", "N/A")
        claim_date = claim.get("claimDate", "N/A")
        claimant = claim.get("claimant", "N/A")

        for review in claim.get("claimReview", []):
            publisher = review["publisher"].get("name", "N/A")
            site = review["publisher"].get("site", "N/A")
            review_date = review.get("reviewDate", "N/A")
            title = review.get("title", "N/A")
            textual_rating = review.get("textualRating", "N/A")
            language_code = review.get("languageCode", "N/A")
            url = review.get("url", "N/A")

            # Get sentiment score for the textual rating
            score = get_sentiment_score(textual_rating)

            formatted_data.append(
                [
                    text,
                    claim_date,
                    claimant,
                    publisher,
                    site,
                    review_date,
                    title,
                    textual_rating,
                    score,
                    language_code,
                    url,
                ]
            )

    # Create a pandas DataFrame
    df = pd.DataFrame(
        formatted_data,
        columns=[
            "Claim Text",
            "Claim Date",
            "Claimant",
            "Publisher",
            "Site",
            "Review Date",
            "Review Title",
            "Textual Rating",
            "Score",
            "Language",
            "URL",
        ],
    )

    print(tabulate(df[["Claim Text", "Textual Rating", "Score"]], headers="keys"))


def main():
    claim = input("Enter the claim to analyze: ")
    print("the claim is: ", claim)
    services = ["google_fact_check"]

    analyzer = ClaimAnalyzer(services)
    result = analyzer.analyze_claim(claim)
    print("result is: ", result)

    # Print results for validation
    print("\nAnalysis Results:")
    if "wikipedia" in services:
        print("\nWikipedia Data:")
        print(result.get("wikipedia_data"))
    if "newsapi" in services:
        print("\nNewsAPI Data:")
        print(result.get("newsapi_data"))
    if "google_fact_check" in services:
        print("\nGoogle Fact Check Data:")
        format_google_fact_check_results(result.get("google_fact_check_data"))
    print("\nMatches in Wikipedia and NewsAPI Data:")
    for match in result.get("wiki_news_matches"):
        print(f"- {match}")
    if "google_fact_check" in services:
        print("\nMatches in Google Fact Check Data:")
        for match in result.get("google_fact_matches"):
            print(f"- {match}")


if __name__ == "__main__":
    main()
