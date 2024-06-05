import hashlib
import logging
import time

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from scripts.path_operators import get_datasets_dir

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("fact_checker.log"), logging.StreamHandler()],
)


def setup_driver():
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )
    return driver


def handle_cookie_consent(driver):
    try:
        cookie_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "CybotCookiebotDialogBodyButtonDecline"))
        )
        cookie_button.click()
    except Exception as e:
        logging.warning(f"No cookie consent dialog: {e}")


def handle_steady_floating_button(driver):
    try:
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "steady-floating-button"))
        ).click()
        time.sleep(1)
    except Exception as e:
        logging.warning(f"No steady floating button: {e}")


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

        title = title_element.text.strip() if title_element else ""
        date = date_element.text.strip() if date_element else ""
        unique_id = (
            hashlib.md5(f"{title}{date}".encode()).hexdigest()
            if date
            else hashlib.md5(title.encode()).hexdigest()
        )

        card_info = {
            "id": unique_id,
            "title": title,
            "date": date,
            "source": (
                source_element.text.strip().replace("Fonte:", "").strip()
                if source_element
                else ""
            ),
            "read_more_link": read_more_element["href"] if read_more_element else "",
            "author": author_element.text.strip() if author_element else "",
            "party": party_element.text.strip() if party_element else "",
            "verdict": "",
        }
        cards.append(card_info)
    return cards


def load_all_cards(driver, max_cards=None):
    all_cards = []
    loaded_card_titles = set()
    while True:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        new_cards = extract_fact_checking_cards_with_verdict(soup)
        for card in new_cards:
            if card["title"] not in loaded_card_titles:
                all_cards.append(card)
                loaded_card_titles.add(card["title"])
        if max_cards and len(all_cards) >= max_cards:
            break
        try:
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//button[contains(@class, "btn isTag isOutline")]')
                )
            ).click()
            time.sleep(0.5)
        except Exception as e:
            logging.info(f"No more cards to load: {e}")
            break
    return all_cards


def find_verdict(driver, card, card_index):
    title = card["title"]
    logging.info(f"Processing card {card_index} with title '{title}'")
    try:
        start_time = time.time()

        # Construct the XPath to find the article based on the title
        article_xpath = f'//h3[contains(text(),"{title}")]/ancestor::article'
        article = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, article_xpath))
        )

        logging.info(
            f"Found article for card {card_index} in {time.time() - start_time:.2f} seconds"
        )

        # Find the button within the article and click it
        verdict_button_xpath = "//button[contains(@class, 'pt-8') and .//div[contains(text(), 'Vai al verdetto')]]"
        verdict_button = WebDriverWait(article, 10).until(
            EC.element_to_be_clickable((By.XPATH, verdict_button_xpath))
        )
        verdict_button.click()

        logging.info(
            f"Clicked verdict button for card {card_index} in {time.time() - start_time:.2f} seconds"
        )

        # Wait for the verdict to appear, assume change in class or structure
        WebDriverWait(driver, 10).until(
            lambda driver: "isVerdetto" in article.get_attribute("class")
        )

        logging.info(
            f"Verdict appeared for card {card_index} in {time.time() - start_time:.2f} seconds"
        )

        # Extract the verdict text from the updated article element
        verdict_xpath = ".//h3[contains(@class, 'declaration line-clamp-6') and contains(@class, 'text-white')]"
        verdict_text_element = WebDriverWait(article, 10).until(
            EC.visibility_of_element_located((By.XPATH, verdict_xpath))
        )
        card["verdict"] = verdict_text_element.text.strip()

        logging.info(
            f"Extracted verdict for card {card_index} in {time.time() - start_time:.2f} seconds"
        )
    except TimeoutException:
        logging.error(
            f"Timeout while trying to process card {card_index} with title '{title}'."
        )
        card["verdict"] = "No verdict available"
    except NoSuchElementException:
        logging.error(
            f"Element not found error while processing card {card_index} with title '{title}'."
        )
        card["verdict"] = "No verdict available"
    except Exception as e:
        logging.error(f"An error occurred while processing card {card_index}: {e}")
        card["verdict"] = "No verdict available"

    return card


def main():
    df_existing = pd.DataFrame()
    driver = setup_driver()
    url = "https://pagellapolitica.it/fact-checking"
    driver.get(url)
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "col-span-4"))
    )
    handle_cookie_consent(driver)
    handle_steady_floating_button(driver)
    file_path = get_datasets_dir("fact_checking_with_verdict.parquet")
    existing_ids = set()
    try:
        df_existing = pd.read_parquet(file_path)
        existing_ids = set(df_existing["id"])
        logging.info(f"Loaded {len(existing_ids)} existing IDs from {file_path}")
    except FileNotFoundError:
        logging.info(
            f"No existing file found at {file_path}. All IDs will be considered new."
        )

    max_cards = 50
    all_cards = load_all_cards(driver, max_cards)
    new_cards = [card for card in all_cards if card["id"] not in existing_ids]
    for i, card in enumerate(new_cards):
        if card["verdict"] == "":
            find_verdict(
                driver, card, i
            )  # Assuming the find_verdict function handles the entire process

    driver.quit()
    new_cards = [card for card in new_cards if card["verdict"] != ""]
    df_new_cards = pd.DataFrame(new_cards)
    if not df_new_cards.empty:
        if not df_existing.empty:
            df_combined = pd.concat([df_existing, df_new_cards]).drop_duplicates(
                subset="id"
            )
            df_combined.to_parquet(file_path, index=False, engine="pyarrow")
            logging.info(
                f"Appended {len(df_new_cards)} new cards. Total {len(df_combined)} entries now."
            )
        else:
            df_new_cards.to_parquet(file_path, index=False, engine="pyarrow")
            logging.info(f"Saved {len(df_new_cards)} new cards to {file_path}")
    else:
        logging.info("No new cards to process.")
    logging.info(df_new_cards)


if __name__ == "__main__":
    main()
