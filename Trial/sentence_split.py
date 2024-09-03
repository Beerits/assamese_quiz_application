import sqlite3

def create_sentences_table(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create a new table for sentences
    cursor.execute('''CREATE TABLE IF NOT EXISTS sentences_table (
                        id INTEGER,
                        sentence TEXT,
                        FOREIGN KEY (id) REFERENCES scraped_data(id)
                    )''')
    conn.commit()
    return conn, cursor

def split_and_store_sentences(cursor, conn):
    try:
        # Fetch content and corresponding id from scraped_data
        cursor.execute('SELECT id, content FROM scraped_data')
        rows = cursor.fetchall()

        for row in rows:
            scraped_id = row[0]
            content = row[1]

            # Split the content into sentences at '।'
            sentences = content.split('।')

            # Store each sentence into the new table
            for sentence in sentences:
                sentence = sentence.strip()  # Clean up any extra whitespace
                if sentence:  # Skip empty sentences
                    cursor.execute('''INSERT INTO sentences_table (id, sentence)
                                      VALUES (?, ?)''', (scraped_id, sentence))
        conn.commit()

    except sqlite3.Error as e:
        print(f"Error working with database: {e}")

    finally:
        conn.close()

# Example usage:
db_name = 'scraped_data.db'  # Your existing database name

# Create the sentences table
conn, cursor = create_sentences_table(db_name)

# Split the content from scraped_data and store sentences
split_and_store_sentences(cursor, conn)
