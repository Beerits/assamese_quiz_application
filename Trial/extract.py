import requests
from bs4 import BeautifulSoup
import time
import hashlib
import sqlite3
import datetime

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
        # Send a GET request to the main webpage
        response = requests.get(url)
        response.raise_for_status()

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract all links that contain 'www.asomiyapratidin.in'
        anchors = soup.find_all('a', href=True)
        links = [a['href'] for a in anchors if 'www.asomiyapratidin.in' in a['href']]

        for link in links:
            # Terminate if the max duration has been reached
            elapsed_time = time.time() - start_time
            if elapsed_time > max_duration * 60:
                print("Time limit reached. Terminating the script.")
                break

            try:
                # Fetch the linked page
                link_response = requests.get(link)
                link_response.raise_for_status()

                # Parse the linked page
                link_soup = BeautifulSoup(link_response.content, 'html.parser')

                # Find all <p> tags and extract text
                paragraphs = link_soup.find_all('p')
                text_content = "\n".join([p.get_text() for p in paragraphs])

                # Store the data in the database
                store_data(cursor, conn, link, text_content)

                print(f"Data stored from {link}")
                time.sleep(1)  # To avoid overloading the server

            except requests.exceptions.RequestException as e:
                print(f"Failed to extract from {link}: {e}")

    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")
    finally:
        # Close the database connection
        conn.close()

# Example usage:
url = 'https://www.asomiyapratidin.in/'  # Replace with the actual URL
db_name = 'scraped_data.db'  # SQLite database name
max_duration = 2  # Specify the maximum duration in minutes

extract_links_and_paragraphs(url, db_name, max_duration)
