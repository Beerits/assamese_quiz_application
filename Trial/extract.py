import requests
import time
import hashlib
import sqlite3
import datetime
from html.parser import HTMLParser
from urllib.parse import urljoin

class MyHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self.paragraphs = []
        self.in_paragraph = False

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            href = dict(attrs).get('href')
            if href and 'www.asomiyapratidin.in' in href:
                self.links.append(href)
        elif tag == 'p':
            self.in_paragraph = True

    def handle_data(self, data):
        if self.in_paragraph:
            self.paragraphs.append(data.strip())

    def handle_endtag(self, tag):
        if tag == 'p':
            self.in_paragraph = False

def create_database(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute('''CREATE TABLE IF NOT EXISTS scraped_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        site_link TEXT,
                        hash TEXT UNIQUE,
                        content TEXT,
                        timestamp TEXT
                    )''')
    conn.commit()
    return conn, cursor

def store_data(cursor, conn, site_link, content):
    try:
        # Remove unwanted text '© asomiyapratidin 2024' from the content
        content = content.replace('© asomiyapratidin 2024', '').strip()

        # Skip if content is empty
        if not content:
            print(f"Skipping {site_link} due to empty content.")
            return

        # Create a unique hash for each site link
        link_hash = hashlib.md5(site_link.encode()).hexdigest()

        # Get the current timestamp
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Insert the data into the database
        cursor.execute('''INSERT OR IGNORE INTO scraped_data (site_link, hash, content, timestamp) 
                          VALUES (?, ?, ?, ?)''', (site_link, link_hash, content, timestamp))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error inserting data into the database: {e}")


def extract_links_and_paragraphs(url, db_name, max_duration):
    conn, cursor = create_database(db_name)
    start_time = time.time()

    try:
        response = requests.get(url)
        response.raise_for_status()
        parser = MyHTMLParser()
        parser.feed(response.text)

        for link in parser.links:
            elapsed_time = time.time() - start_time
            if elapsed_time > max_duration * 60:
                print("Time limit reached. Terminating the script.")
                break

            try:
                full_link = urljoin(url, link)  # Handle relative links
                link_response = requests.get(full_link)
                link_response.raise_for_status()

                link_parser = MyHTMLParser()
                link_parser.feed(link_response.text)
                text_content = "\n".join(link_parser.paragraphs)

                store_data(cursor, conn, full_link, text_content)
                print(f"Data stored from {full_link}")
                time.sleep(1)

            except requests.exceptions.RequestException as e:
                print(f"Failed to extract from {link}: {e}")

    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")
    finally:
        conn.close()

# Example usage:
url = 'https://www.asomiyapratidin.in/'  # Replace with the actual URL
db_name = 'scraped_data.db'  # SQLite database name
max_duration = 2  # Specify the maximum duration in minutes

extract_links_and_paragraphs(url, db_name, max_duration)
