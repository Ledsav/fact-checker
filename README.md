# Fact Checker Database Generation

This project involves creating a database of fact-checking entries for Italian politicians using web scraping, cloud storage, and natural language processing (NLP). It is a Python-based project focused on the database generation part of a complete fact-checking application.

## Features

- **Web Scraping**: Extract fact-checking cards from a website.
- **Data Processing**: Clean and standardize data, classify verdicts, and compute scores.
- **Cloud Storage**: Store processed data in Firebase Firestore.
- **Visualization**: Plot and analyze credibility scores of politicians and parties.

## Installation

1. **Clone the repository**:
    ```sh
    git clone https://github.com/your-username/fact-checker.git
    cd fact-checker
    ```

2. **Create and activate a virtual environment**:
    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3. **Install the dependencies**:
    ```sh
    pip install -r requirements.txt
    ```

4. **Set up Firebase**:
    - Add your Firebase configuration to a `key_firebase.json` file in the project root.

## Usage

### Data Scraping and Processing

1. **Run the scraper**:
    ```sh
    python scripts/scraping.py
    ```

    This script will:
    - Set up a headless Chrome driver.
    - Handle cookie consent and floating button on the website.
    - Extract fact-checking cards and their verdicts.
    - Save the data to a Parquet file.

2. **Process the data**:
    ```sh
    python scripts/processing.py
    ```

    This script will:
    - Load the dataset from the Parquet file.
    - Clean and standardize the data.
    - Classify verdicts and compute scores.
    - Save processed data to a new Parquet file.
    - Create subcollections for easier access to author and party information.

### Data Visualization

1. **Run the visualization script**:
    ```sh
    python scripts/analysis.py
    ```

    This script will:
    - Load the processed dataset.
    - Generate plots for credibility scores by politicians and parties.
    - Display interactive visualizations.

### Cloud Storage

1. **Upload data to Firebase**:
    ```sh
    python scripts/storage.py
    ```

    This script will:
    - Initialize Firebase connection.
    - Load datasets from Parquet files.
    - Upsert data to Firebase Firestore.
    - Store processed data, author averages, and party averages in separate collections.

## Project Structure

```
fact-checker/
│
├── datasets/                           # Parquet files
│   ├── average_by_author.parquet
│   ├── average_by_party.parquet
│   ├── fact_checking_with_verdict.parquet
│   ├── processed_fact_checking_with_scores.parquet
│
├── logs/                               # Log files
│   └── fact_checker.log
│
├── scripts/                            # Python scripts for various tasks
│   ├── analysis.py                     # Data visualization script
│   ├── path_operators.py               # Utility functions for path operations
│   ├── processing.py                   # Data processing script
│   ├── prototyping.py                  # Prototyping and testing script
│   ├── scraping.py                     # Web scraping script
│   ├── storage.py                      # Script to upload data to Firebase
│
├── .gitignore                          # Git ignore file
├── config.py                           # Configuration file
├── key_firebase.json                   # Firebase configuration file (add this file)
├── requirements.txt                    # Python dependencies
├── TODO.txt                            # To-do list
└── README.md                           # Project documentation
```

## Dependencies

- Python 3.8+
- BeautifulSoup4
- Selenium
- pandas
- pyarrow
- firebase-admin
- matplotlib
- seaborn
- plotly
- webdriver-manager

## Contributing

Contributions are welcome! Please feel free to submit a pull request.

## License

This project is licensed under the MIT License.

## Contact

For any inquiries, please contact [Ledsav](alberto.valdes.rey.official@gmail.com)
