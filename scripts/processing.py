import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scripts.path_operators import get_datasets_dir

NEGATIVE_KEYWORDS = [
    "falsa", "scorretta", "sbagliata", "non è supportata", "non stanno proprio",
    "esagerata", "imprecisa", "parecchio esagerato", "fuorviante", "omette",
    "non la racconta giusta", "fa confusione", "smentiscono", "danno torto",
    "confonde", "completamente sbagliato", "sbaglia", "torto", "dà meriti che non ha",
]

NEUTRAL_KEYWORDS = [
    "esagera", "troppo semplice", "parziale", "troppo ottimista", "semplificazione",
    "eccessivo", "dipende", "provvisori",
]

POSITIVE_KEYWORDS = [
    "verità", "corretta", "corrette", "più alte", "Sì", "ha ragione",
    "sostanzialmente corretta", "danno ragione", "sono corretti", "supportata dai fatti",
    "attendibile", "previsioni scientifiche", "ordine di grandezza è corretto",
    "cifre esatte",
]

MONTHS_IT = {
    "gennaio": "January", "febbraio": "February", "marzo": "March",
    "aprile": "April", "maggio": "May", "giugno": "June",
    "luglio": "July", "agosto": "August", "settembre": "September",
    "ottobre": "October", "novembre": "November", "dicembre": "December",
    "GENNAIO": "January", "FEBBRAIO": "February", "MARZO": "March",
    "APRILE": "April", "MAGGIO": "May", "GIUGNO": "June",
    "LUGLIO": "July", "AGOSTO": "August", "SETTEMBRE": "September",
    "OTTOBRE": "October", "NOVEMBRE": "November", "DICEMBRE": "December",
}

PARTY_ORIENTATION = {
    'Alleanza Verdi e Sinistra': 'sinistra', 'Azione': 'destra', 'Europa Verde': 'sinistra',
    'Forza Italia': 'destra', 'Fratelli d\'Italia': 'destra', 'Impegno Civico': 'destra',
    'Italia Viva': 'centro', 'Lega': 'destra', 'Liberi e Uguali': 'sinistra',
    'Movimento 5 Stelle': 'sinistra', 'Partito Democratico': 'sinistra',
    'Più Europa': 'sinistra', 'Sinistra Italiana': 'sinistra', 'Tecnico': 'centro',
}

IMAGE_EXCEPTIONS = {
    'Partito Democratico': 'https://upload.wikimedia.org/wikipedia/it/thumb/4/4a/Logo_Partito_Democratico.svg/150px-Logo_Partito_Democratico.svg.png',
    'Impegno Civico': 'https://upload.wikimedia.org/wikipedia/it/thumb/a/a4/Impegno_Civico_%28Italia%2C_2023%29_-_Logo.png/220px-Impegno_Civico_%28Italia%2C_2023%29_-_Logo.png',
    'Gianantonio Da Re': 'https://www.europarl.europa.eu/mepphoto/197608.jpg',
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

    for it_month, en_month in MONTHS_IT.items():
        date_str = date_str.replace(it_month, en_month)

    for date_format in (
            "%d %B %Y", "%d %b %Y", "%B %Y", "%b %Y",
            "%d %B", "%d %b", "%B", "%b",
    ):
        try:
            parsed_date = datetime.strptime(date_str, date_format)
            if parsed_date.year == 1900:
                parsed_date = parsed_date.replace(year=1900)
            if parsed_date.month == 1 and "%B" not in date_format and "%b" not in date_format:
                parsed_date = parsed_date.replace(month=1)
            if parsed_date.day == 1 and "%d" not in date_format:
                parsed_date = parsed_date.replace(day=1)
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return "1900-01-01"  # Return default old date if no format matches


def standardize_party_name(party):
    exceptions = ["e", "d'", "di"]
    words = party.split()
    standardized_words = [word.capitalize() if word.lower() not in exceptions else word.lower() for word in words]
    standardized_words[0] = standardized_words[0].capitalize()
    return ' '.join(standardized_words)


def correct_party_name(party):
    # Define the exceptions that should remain lowercase
    exceptions = ["e", "d'", "di"]

    # Split the party name into individual words
    words = party.split()

    # Standardize each word: capitalize if not an exception, lowercase if it is
    standardized_words = []
    for word in words:
        # Handle "d'" specifically
        if re.match(r"d'[A-Za-z]+", word):
            standardized_words.append("d'" + word[2:].capitalize())
        elif word.lower() in exceptions:
            standardized_words.append(word.lower())
        else:
            standardized_words.append(word.capitalize())

    # Ensure the first word is capitalized
    if standardized_words:
        standardized_words[0] = standardized_words[0].capitalize()

    return ' '.join(standardized_words)


def correct_party_names(df, column_name):
    df[column_name] = df[column_name].apply(correct_party_name)
    return df


def add_party_orientation(df):
    df['orientation'] = df['party'].map(PARTY_ORIENTATION)
    return df


def fetch_wikipedia_image(query):
    def get_image_from_page(page_url):
        page_response = requests.get(page_url)
        soup = BeautifulSoup(page_response.content, 'html.parser')

        # Check common infobox classes
        infobox = soup.find('table', class_='infobox') or soup.find('table', 'vcard')
        if infobox:
            img_tag = infobox.find('img')
            if img_tag:
                return 'https:' + img_tag['src']
        return None

    def is_disambiguation_page(soup):
        return soup.find('div', class_='avviso-disambigua')

    def extract_disambiguation_links(soup):
        disambiguation_section = soup.find('div', class_='mw-parser-output')
        links = []
        if disambiguation_section:
            for li in disambiguation_section.find_all('li'):
                a_tag = li.find('a', href=True)
                description = li.get_text()
                if a_tag and description:
                    links.append((a_tag['href'], description))
        return links

    def resolve_disambiguation_page(page_url):
        page_response = requests.get(page_url)
        soup = BeautifulSoup(page_response.content, 'html.parser')

        disambiguation_links = extract_disambiguation_links(soup)

        if disambiguation_links:
            # Prioritize the links by relevance
            exact_matches = [href for href, description in disambiguation_links if
                             'partito politico italiano attivo' in description.lower()]
            generic_matches = [href for href, description in disambiguation_links if
                               'partito politico' in description.lower()]

            # Select the most relevant link
            if exact_matches:
                return f"https://it.wikipedia.org{exact_matches[0]}"
            elif generic_matches:
                return f"https://it.wikipedia.org{generic_matches[-1]}"

            # Check for "Politica" section links if no matches are found
            politica_heading = soup.find(id='Politica')
            if politica_heading:
                politica_list = politica_heading.find_next('ul')
                if politica_list:
                    politica_links = politica_list.find_all('li')
                    if politica_links:
                        for li in reversed(politica_links):
                            description = li.get_text()
                            a_tag = li.find('a', href=True)
                            if 'partito politico' in description.lower() and a_tag:
                                return f"https://it.wikipedia.org{a_tag['href']}"

            # If still no match, return the last valid link
            return f"https://it.wikipedia.org{disambiguation_links[-1][0]}"

        return None

    print('query:', query)

    if query in IMAGE_EXCEPTIONS:
        return IMAGE_EXCEPTIONS[query]

    page_url = f"https://it.wikipedia.org/wiki/{query}"
    print(page_url)
    page_response = requests.get(page_url)
    soup = BeautifulSoup(page_response.content, 'html.parser')

    if is_disambiguation_page(soup):
        print('disambiguation')
        resolved_url = resolve_disambiguation_page(page_url)
        if resolved_url:
            img_url = get_image_from_page(resolved_url)
            if img_url:
                print(img_url)
                return img_url
    else:
        # Handle normal page
        img_url = get_image_from_page(page_url)
        if img_url:
            print(img_url)
            return img_url

    print('Nothing')
    return None


def process_dataset(df):
    df = df[df["verdict"] != "No verdict available"]
    df = df.dropna(subset=["verdict", "author", "party"])
    df = df[df["author"].str.strip() != ""]
    df = df[df["party"].str.strip() != ""]
    df = df.drop_duplicates(subset=["id"])
    df["date"] = df["date"].apply(standardize_date)
    df["score"] = df["verdict"].apply(classify_verdict)
    df["party"] = df["party"].apply(correct_party_name)
    df = add_party_orientation(df)
    df = df.sort_values(by="date", ascending=False)
    return df


def save_dataset(df, file_path):
    df.to_parquet(file_path, index=False, engine="pyarrow")


def create_grouped_parquets(df):
    df_author = df.groupby(['author', 'party']).agg(
        average_score=('score', 'mean'),
        count=('score', 'size')
    ).reset_index()
    df_author = add_party_orientation(df_author)
    df_author['author_image'] = df_author['author'].apply(fetch_wikipedia_image)

    df_party = df.groupby('party').agg(
        average_score=('score', 'mean'),
        count=('score', 'size')
    ).reset_index()
    df_party = add_party_orientation(df_party)
    df_party['party_image'] = df_party['party'].apply(fetch_wikipedia_image)

    save_dataset(df_party, get_datasets_dir("average_by_party.parquet"))
    save_dataset(df_author, get_datasets_dir("average_by_author.parquet"))


def main():
    input_path = get_datasets_dir("fact_checking_with_verdict.parquet")
    output_path = get_datasets_dir("processed_fact_checking_with_scores.parquet")
    df = load_dataset(input_path)
    df = process_dataset(df)
    save_dataset(df, output_path)
    create_grouped_parquets(df)
    print(df)  # Optional for debugging


if __name__ == "__main__":
    main()
