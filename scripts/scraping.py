import time

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from scripts.path_operators import get_datasets_dir

# URL of the webpage to retrieve
url = "https://pagellapolitica.it/fact-checking"

# Set up Selenium WebDriver
options = Options()
options.headless = True  # Run in headless mode (no browser window)
driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()), options=options
)

# Load the webpage
driver.get(url)
WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.CLASS_NAME, "col-span-4"))
)

# Handle the cookie consent dialog if present
try:
    cookie_button = driver.find_element(By.ID, "CybotCookiebotDialogBodyButtonDecline")
    cookie_button.click()
    WebDriverWait(driver, 10)
except Exception as e:
    print(f"No cookie consent dialog: {e}")

# Check for and close the "Steady Floating Button" if present
try:
    time.sleep(5)  # Wait for the button to spawn
    steady_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "steady-floating-button"))
    )
    steady_button.click()
    time.sleep(1)  # Wait for the button to close
except Exception as e:
    print(f"No steady floating button: {e}")


# Function to extract fact-checking card information
def extract_fact_checking_cards_with_verdict(soup):
    cards = []
    card_elements = soup.find_all("li", class_="col-span-4 flex")
    for card_nth in card_elements:
        article_nth = card_nth.find("article", class_="card")

        title_element = article_nth.find("h3", class_="declaration")
        date_element = article_nth.find("div", class_="declaration-date")
        source_element = article_nth.find("div", class_="declaration-fonte")
        read_more_element = article_nth.find("a", class_="btn")
        author_element = article_nth.find("h4", class_="declaration-author")
        party_element = article_nth.find("p", class_="declaration-date")

        card_info = {
            "title": title_element.text.strip() if title_element else "",
            "date": date_element.text.strip() if date_element else "",
            "source": (
                source_element.text.strip().replace("Fonte:", "").strip()
                if source_element
                else ""
            ),
            "read_more_link": read_more_element["href"] if read_more_element else "",
            "author": author_element.text.strip() if author_element else "",
            "party": party_element.text.strip() if party_element else "",
            "verdict": "",  # Placeholder for the verdict to be filled later
        }
        cards.append(card_info)
    return cards


def load_all_cards(driver, max_cards=None):
    all_cards = []
    loaded_card_titles = set()  # Track titles of loaded cards to avoid duplicates

    while True:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        new_cards = extract_fact_checking_cards_with_verdict(soup)

        # Add only new unique cards to the list
        for card in new_cards:
            if card["title"] not in loaded_card_titles:
                all_cards.append(card)
                loaded_card_titles.add(card["title"])

        # Check if we've reached the max number of cards
        if max_cards and len(all_cards) >= max_cards:
            all_cards = all_cards[:max_cards]
            break

        # Try to click the "Carica altre dichiarazioni" button
        try:
            load_more_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//button[contains(@class, "btn isTag isOutline")]')
                )
            )
            load_more_button.click()
            time.sleep(0.5)  # Wait for new cards to load
        except Exception as e:
            print(f"No more cards to load: {e}")
            break

    return all_cards


# Extract all fact-checking cards
max_cards = 200  # Set this to the desired maximum number of cards or None to load all
all_cards = load_all_cards(driver, max_cards)

# Click all "Vai al verdetto" buttons and extract verdicts
for i, card in enumerate(all_cards):
    if card["verdict"] == "":  # Skip cards with already extracted verdicts
        try:
            # Find the "Vai al verdetto" button within the context of the card
            title = card["title"]
            article = None
            try:
                article = driver.find_element(
                    By.XPATH, f'//h3[contains(text(),"{title}")]/ancestor::article'
                )
            except:
                try:
                    article = driver.find_element(
                        By.XPATH,
                        f'//h3[contains(text(),"{title.split()[0]}")]/ancestor::article',
                    )
                except Exception as e:
                    print(f"Error finding article for card {i}: {e}")
                    continue

            verdict_button = article.find_element(
                By.XPATH, './/div[contains(@class, "declaration-turn")]'
            )

            # Scroll to the button to make sure it's not blocked
            driver.execute_script("arguments[0].scrollIntoView(true);", verdict_button)
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable(verdict_button))
            verdict_button.click()
            time.sleep(0.1)  # Wait for the verdict to be revealed

            # Extract the verdict
            verdict_element = None
            try:
                verdict_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            f'//h3[contains(text(),"{title}")]/ancestor::article//h3[contains(@class, "declaration line-clamp-6 text-white")]',
                        )
                    )
                )
            except:
                try:
                    verdict_element = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (
                                By.XPATH,
                                f'//h3[contains(text(),"{title.split()[0]}")]/ancestor::article//h3[contains(@class, "declaration line-clamp-6 text-white")]',
                            )
                        )
                    )
                except Exception as e:
                    print(f"Error finding verdict for card {i}: {e}")
                    card["verdict"] = "No verdict available"
                    continue

            card["verdict"] = (
                verdict_element.text.strip()
                if verdict_element
                else "No verdict available"
            )
        except Exception as e:
            print(f"Error extracting verdict for card {i}: {e}")
            card["verdict"] = "No verdict available"

# Close the Selenium WebDriver
driver.quit()

# Filter out cards with empty verdicts
all_cards = [card for card in all_cards if card["verdict"] != ""]

# Create a DataFrame with the extracted information
df_with_verdict = pd.DataFrame(all_cards)

# Save the DataFrame to a Parquet file
df_with_verdict.to_parquet(
    get_datasets_dir() / "fact_checking_with_verdict.parquet",
    index=False,
    engine="pyarrow",
)

# Show the DataFrame
print(df_with_verdict)
