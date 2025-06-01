import requests
import psycopg2
from psycopg2 import sql
import random
import time
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class GetStreamers:
    def __init__(self, client_id, token):
        self.base_url = "https://streamscharts.com/api/jazz"
        self.platform = "twitch"
        self.testing_mode = "&testing_mode=true"
        self.streamers = []
        self.history_data = {}
        self.headers = {
            "Client-ID": client_id,
            "Token": token
        }

    def scrape(self):
        logging.info("Requesting Twitch streamers (limit: 20, sorted by average_viewers)...")
        response = requests.get(
            f"{self.base_url}/channels?platform={self.platform}&time=7-days{self.testing_mode}",
            headers=self.headers
        )
        logging.debug(f"Scrape response status: {response.status_code}")
        if response.status_code == 200:
            data = response.json().get('data', [])
            # Sort by average_viewers descending, then take top 20
            sorted_data = sorted(data, key=lambda s: s.get('average_viewers', 0), reverse=True)
            top_20 = sorted_data[:20]
            self.streamers = [s['channel_name'] for s in top_20]
            logging.info(f"Found {len(self.streamers)} streamers (top 20 by average_viewers).")
            logging.debug(f"Streamer list: {self.streamers}")
        else:
            logging.error(f"Failed to fetch streamers: {response.status_code} {response.text}")
            self.streamers = []
        return self.streamers

    def history(self):
        logging.info("Fetching history for each streamer at multiple time points...")
        time_periods = ["7-days", "last-month", "last-year"]
        for streamer in self.streamers:
            self.history_data[streamer] = []
            for period in time_periods:
                logging.debug(f"Requesting history for streamer: {streamer} at period: {period}")
                response = requests.get(
                    f"{self.base_url}/channels/{streamer}?platform={self.platform}&time={period}{self.testing_mode}",
                    headers=self.headers
                )
                logging.debug(f"History response status for {streamer} ({period}): {response.status_code}")
                if response.status_code == 200:
                    data = response.json().get('data', {})
                    # Attach the period as the 'date' for clarity
                    if data:
                        data['date'] = period
                        self.history_data[streamer].append(data)
                        logging.info(f"History record for {streamer} ({period}) added.")
                else:
                    logging.error(f"Failed to fetch history for {streamer} ({period}): {response.status_code} {response.text}")
                time.sleep(0.2)  # Be polite to the API
        return self.history_data

class CompileData:
    def __init__(self, db_name="twitchdata", db_user="postgres", db_host="localhost", db_port=5432):
        self.db_name = db_name
        self.db_user = db_user
        self.db_host = db_host
        self.db_port = db_port
        self.db_password = os.getenv("PGPASSWORD")

    def database(self, streamer_tables):
        logging.info("Checking/creating PostgreSQL tables for each streamer...")
        conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )
        c = conn.cursor()
        for streamer in streamer_tables:
            table_name = f"streamer_{streamer.lower()}"
            c.execute(sql.SQL('''
                CREATE TABLE IF NOT EXISTS {} (
                    date TEXT,
                    average_viewers INTEGER,
                    stream_days INTEGER,
                    PRIMARY KEY (date)
                )
            ''').format(sql.Identifier(table_name)))
        conn.commit()
        c.close()
        conn.close()
        logging.info("All streamer tables ready.")

    def format(self, history_data):
        logging.info("Formatting history data for per-streamer table insertion...")
        streamer_lines = {}
        for streamer, records in history_data.items():
            table_name = f"streamer_{streamer.lower()}"
            lines = []
            for record in records:
                avg_viewers = record.get('average_viewers')
                stream_days = record.get('stream_days')
                date = record.get('date')
                line = (date, avg_viewers, stream_days)
                lines.append(line)
            streamer_lines[table_name] = lines
        logging.debug(f"Formatted data for {len(streamer_lines)} streamer tables.")
        return streamer_lines

    def append(self, streamer_lines):
        logging.info(f"Appending data to each streamer's table in PostgreSQL...")
        conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )
        c = conn.cursor()
        for table_name, lines in streamer_lines.items():
            for line in lines:
                try:
                    c.execute(
                        sql.SQL('INSERT INTO {} (date, average_viewers, stream_days) VALUES (%s, %s, %s) ON CONFLICT (date) DO NOTHING')
                        .format(sql.Identifier(table_name)),
                        line
                    )
                except Exception as e:
                    logging.error(f"Error inserting line {line} into {table_name}: {e}")
                    conn.rollback()
        conn.commit()
        c.close()
        conn.close()
        logging.info("All data appended to streamer tables.")

class Test:
    def __init__(self, db_name="twitchdata", db_user="postgres", db_host="localhost", db_port=5432):
        self.db_name = db_name
        self.db_user = db_user
        self.db_host = db_host
        self.db_port = db_port
        self.db_password = os.getenv("PGPASSWORD")

    def testdata(self):
        logging.info("Selecting a random data line from the database for testing...")
        conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )
        c = conn.cursor()
        c.execute('SELECT streamer, date, average_viewers FROM streamer_history ORDER BY RANDOM() LIMIT 1')
        row = c.fetchone()
        c.close()
        conn.close()
        logging.debug(f"Random test data: {row}")
        return row

    def testcase(self, data_line):
        logging.info(f"Validating presence of data line in database: {data_line}")
        conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )
        c = conn.cursor()
        c.execute('SELECT 1 FROM streamer_history WHERE streamer=%s AND date=%s AND average_viewers=%s', data_line)
        result = c.fetchone()
        c.close()
        conn.close()
        if result:
            logging.info("Test case passed: Data line found.")
        else:
            logging.warning("Test case failed: Data line not found.")
        return result is not None

def main():
    load_dotenv()
    client_id = os.getenv("CLIENT_ID")
    token = os.getenv("TOKEN")
    if not client_id or not token:
        logging.error("CLIENT_ID or TOKEN not found in environment. Exiting.")
        return
    gs = GetStreamers(client_id=client_id, token=token)
    streamers = gs.scrape()
    print("Streamers:", streamers)
    history = gs.history()
    cd = CompileData()
    cd.database(streamers)
    streamer_lines = cd.format(history)
    print("Formatted streamer lines:", streamer_lines)
    cd.append(streamer_lines)
    # Uncomment below to test reading back
    # test = Test()
    # data_line = test.testdata()
    # print("Test line:", data_line)
    # print("Test case result:", test.testcase(data_line))

if __name__ == "__main__":
    main()
