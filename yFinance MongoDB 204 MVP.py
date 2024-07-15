import os
import yfinance as yf
import pandas as pd
import pymongo
from pymongo import MongoClient
from datetime import datetime, date, timedelta, timezone
from pprint import pp
from configparser import ConfigParser
from pymongo.errors import ConnectionFailure
import logging
import time
from datetime import datetime
from halo import Halo
from termcolor import colored
import pandas_market_calendars as mcal
import pytz
import threading
import sys
from dotenv import load_dotenv

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class InitEngine:
    def __init__(self):
        # Initalize status variable for main_menu
        self.status_message = ""

        # Initalize program connection, classes and main menu
        self.mongodb_config()
        self.connect_mongodb()
        self.data_updater = DataUpdater(self.db, self.stock_list)
        self.scheduler = Scheduler(self.data_updater.update_data)
        self.portfolio_manager = PortfolioManager(self.portfolio_db, self.stock_list, self.main_menu, self.clear_screen)
        self.search_mdb = SearchMDB(self.client, self.clear_screen)
        self.main_menu()

    def mongodb_config(self):
        load_dotenv('mongodbconfig.env')
        self.username = os.getenv('MONGODB_USERNAME')
        password = os.getenv('MONGODB_PASSWORD')
        self.cluster = os.getenv('MONGODB_CLUSTER')
        cluster_url = f"mongodb+srv://{self.username}:{password}@{self.cluster}.zcka1qs.mongodb.net/?retryWrites=true&w=majority"
        # MongoDB connection options
        connection_options = {
            "connectTimeoutMS": 5000,  # 5 seconds
            "socketTimeoutMS": 5000,  # 5 seconds
            "retryWrites": True,
            "retryReads": True,
            "w": "majority"
        }
        self.client = MongoClient(cluster_url, serverSelectionTimeoutMS=10000, **connection_options)

    def connect_mongodb(self):
        # Attempt to connect with retries
        connected = False
        retries = 0
        max_retries = 3
        spinner = Halo(text='Connecting to MongoDB: ', spinner='line')
        spinner.start()
        while not connected and retries < max_retries:
            try:
                self.client.admin.command('ismaster')  # Test the connection
                connected = True
            except ConnectionFailure:
                retries += 1
                logging.warning(f" Connection attempt {retries}/{max_retries} failed. Retrying...")
                time.sleep(2 ** retries)  # Exponential backoff for retries

        spinner.stop()
        if not connected:
            raise ConnectionError("Unable to establish a connection to the MongoDB server.")
        if connected:
            print(f"Successfully Connected as {self.username} to {self.cluster}")

        self.db = self.client['timeseries_db']
        self.portfolio_db = self.client['ticker_db']
        self.stock_list = [ticker['name'] for ticker in self.portfolio_db['tech_tickers'].find()]

    def main_menu(self):
        menu_options = {
            "1": (self.ping_database, "Ping MongoDB Database", 0),
            "2": (self.portfolio_manager.manage_portfolio_submenu, "Manage Ticker Portfolio", 1),
            "3": (self.initial_db, "Initialize OHLC Database", 1),
            "4": (self.data_updater.update_data, "Update OHLC Database", 1),
            "5": (self.scheduler.start, "Start Scheduled Update for OHLC Database", 0),
            "6": (self.search_mdb.find_collections, "Find Collections", 0),
            "7": (self.exit_app, "Exit")
        }

        while True:
            self.clear_screen()
            print(colored("============ Main Menu =============", "red"))
            for key, value in menu_options.items():
                print(f"{key}: {value[1]}")
            print("========== Message Window ==========")
            print(f"{self.status_message}")  # Status line to display feedback
            self.status_message = ""  # Clear status message memory
            print("====================================")

            select = input(colored("Enter selection number: ",'green'))

            if select in menu_options:
                try:
                    menu_options[select][0]()  # Execute the selected option
                    if menu_options[select][2] == 1:
                        input(colored("\nPress Enter to return to the main menu...",'green'))  # Wait for user to review output and confirm
                    self.status_message = f"Information: {menu_options[select][1]}{self.status_message}"
                except Exception as e:
                    self.status_message = f"Error: {str(e)}"
                    input("\nPress Enter to return to the main menu...")  # Wait for user even in case of error
            else:
                self.status_message = "--> Invalid Entry"

    def clear_screen(self):
        import os
        os.system('cls' if os.name == 'nt' else 'clear')

    def exit_app(self):
        print("Exiting Program...")
        exit()

    def ping_database(self):
        try:
            self.client.admin.command('ping')
            self.status_message = ("--> Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)

    def initial_db(self):
        # Important Architechture Note:
        # - inital_db & fetch_store_data logic identifies date gaps and fills them automatically
        while True:
            start_date = input("Enter the start date in YYYY-MM-DD format: ")
            end_date = input("Enter the end date in YYYY-MM-DD format: ")

            try:
                datetime.strptime(start_date, '%Y-%m-%d')
                datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                print("--> Invalid date format. Please enter the date in YYYY-MM-DD format.")
                continue

            for ticker in self.stock_list:
                # Check if the collection for the ticker exists and has data
                data_exists = self.db[ticker].find_one()
                print(f"\n--- {ticker} ---")

                if data_exists:
                    # Get the current start and end dates in the DB
                    print(f"data exists: {data_exists}")
                    first_doc = self.db[ticker].find_one(sort=[("Date", 1)])
                    last_doc = self.db[ticker].find_one(sort=[("Date", -1)])

                    current_start_date = first_doc["Date"].date().strftime('%Y-%m-%d')
                    current_end_date = last_doc["Date"].date().strftime('%Y-%m-%d')

                    if current_start_date > start_date:
                        print(f"Data: {current_start_date} User: {start_date}")
                        # If the current start date is greater than the new start date,
                        # fetch and store data from the new start date to the day before the
                        # current start date
                        self.fetch_store_data(ticker, start_date, pd.to_datetime(
                            current_start_date) - pd.DateOffset(days=1))

                    if current_end_date < end_date:
                        print(f"Data: {current_end_date} User: {end_date}")
                        # If the current end date is less than the new end date, fetch and
                        # store data from the day after the current end date to the new end date
                        self.fetch_store_data(ticker,
                                              pd.to_datetime(current_end_date) + pd.DateOffset(
                                                  days=1), end_date)

                else:
                    # If no data exists for the ticker, fetch and store data from the start
                    # date to the end date
                    print(f"No existing data found for {ticker}: Updating new data...")
                    self.fetch_store_data(ticker, start_date, end_date)
            break

    def fetch_store_data(self, ticker, start_date, end_date):
        # Fetch data from yfinance
        try:
            #data = yf.download(ticker, start=start_date, end=end_date, progress=False)
            data = yf.download(ticker, start=start_date, end=end_date, actions="inline", progress=False)

            data.reset_index(inplace=True)
            data_dict = data.to_dict("records")

            print(f"Number of yFinance requests {ticker}: {len(data)}")

        except ValueError as e:
            print(f"An error occurred while fetching data for {ticker}: {e}")
            return

        # Store data in MongoDB
        if data_dict:
            self.db[ticker].insert_many(data_dict)

class Scheduler:
    def __init__(self, data_updater):
        self.data_updater = data_updater
        self.data_updated = threading.Event()
        self.timer_active = False
        self.timer_lock = threading.Lock()

    def get_next_market_close(self):
        nyse = mcal.get_calendar('NYSE')
        now = pd.Timestamp.now(tz='US/Eastern')
        end_date = now + pd.Timedelta(days=10)
        schedule = nyse.schedule(start_date=now.date().isoformat(), end_date=end_date.date().isoformat())

        # Find the next market close time at or after now
        next_close_times = schedule[schedule.market_close > now]['market_close']
        if not next_close_times.empty:
            next_close = next_close_times.iloc[0]
            return next_close.tz_convert('US/Eastern')
        else:
            # If no market close time is found, return now + 1 day (safeguard)
            print("No market close times found in the specified range.")
            return now + pd.Timedelta(days=1)

    def run_scheduled_tasks(self):
        first_run = True
        while True:
            now = pd.Timestamp.now(tz='US/Eastern')
            next_close = self.get_next_market_close()
            prev_close = next_close - pd.Timedelta(days=1)

            # Calculate the time to wait until 10 PM on the day of the market close
            time_at_10pm = next_close.normalize() + pd.Timedelta(hours=22)  # 10 PM on the same day as market close
            prev_time_at_10pm = time_at_10pm - pd.Timedelta(days=1)
            print(f"time_at_10pm: {time_at_10pm}")

            if first_run:
                prev_day = now - pd.Timedelta(days=1)
                self.data_updater()
                self.data_updated.set()
                print(f"\nFirst run detected: Updating to previous days data ({prev_day.date()})"
                      f"\nScheduling for: {time_at_10pm}")
                first_run = False

            elif now >= prev_close and now <= next_close and now >= prev_time_at_10pm:
                print(f"\nMarket has closed on {prev_close}."
                      f"\nRunning Updater... "
                      f"\nDaily Scheduler Active for: {time_at_10pm}")
                self.data_updater()
                self.data_updated.set()

            # Wait until after running update operation
            sleep_seconds = (time_at_10pm - now).total_seconds()
            time.sleep(max(sleep_seconds, 0))

    def display_timer(self):
        while True:
            self.data_updated.wait()
            next_close = self.get_next_market_close()
            while self.timer_active:
                with self.timer_lock:
                    now = pd.Timestamp.now(tz='US/Eastern')
                    remaining = int((next_close - now).total_seconds())
                    if remaining > 0:
                        sys.stdout.write(
                            f"\rTime until next run: {remaining // 3600} hours, "
                            f"{(remaining % 3600) // 60} minutes, "
                            f"{remaining % 60} seconds ")
                        sys.stdout.flush()
                    else:
                        break
                    time.sleep(1)
            sys.stdout.write("\rTime until next run: ready!          \n")
            self.data_updated.clear()

    def start(self):
        self.timer_active = True
        scheduler_thread = threading.Thread(target=self.run_scheduled_tasks)
        scheduler_thread.daemon = True
        scheduler_thread.start()

        timer_thread = threading.Thread(target=self.display_timer)
        timer_thread.daemon = True
        timer_thread.start()
class DataUpdater:
    def __init__(self, db, stock_list):
        self.db = db
        self.stock_list = stock_list

    def get_appropriate_trading_day(self):
        utc_time = datetime.now(pytz.utc)
        eastern = pytz.timezone('America/New_York')
        eastern_time = utc_time.astimezone(eastern)
        sgt = pytz.timezone('Singapore')
        sg_time = utc_time.astimezone(sgt)

        nyse_calendar = mcal.get_calendar('NYSE')
        today = eastern_time.date()
        trading_days = nyse_calendar.valid_days(start_date=today, end_date=today)

        print("\n====== Timezones =====")
        print("UTC:\t", utc_time)
        print("ET:\t", eastern_time)
        print("SGT:\t", sg_time)

        if trading_days.empty:
            print("Today is not a trading day.")
            return today

        market_open = eastern_time.replace(hour=9, minute=30, second=0)
        market_close = eastern_time.replace(hour=16, minute=30, second=0)

        if eastern_time < market_open:
            # Before market opens, get data from the previous trading day
            return today - timedelta(days=1)
        elif eastern_time > market_close:
            # After market closes, get data from today
            return today
        else:
            # During trading hours, get data from the previous trading day
            return today - timedelta(days=1)

    def update_data(self):
        data_day = self.get_appropriate_trading_day()
        print(f"Data should be fetched for:\t{data_day}")

        for ticker in sorted(self.stock_list):
            print(colored(f"\n----- {ticker} -----",'blue'))
            # Get the latest date in the MongoDB collection
            latest_doc = self.db[ticker].find_one(sort=[("Date", -1)])
            if not latest_doc:
                logging.error(colored(f"No data found for {ticker}: Run Initialization first", 'magenta'))
                continue

            latest_date = latest_doc["Date"].date()
            print(f"latest mongodb document date : {latest_date}")

            # Update if the latest data in the database is older than the most recent avail data day
            if latest_date < data_day:
                try:
                    # Get OHLC Data + Additional Data (Dividends etc)
                    new_data = yf.download(ticker, start=(latest_date + timedelta(days=1)).strftime("%Y-%m-%d"),
                                              end=None, actions="inline", progress=False)
                    print(f"yFinance requests:\t{len(new_data)}")
                    #pp(new_data) #For debugging

                    if new_data.empty:
                        logging.error(f"No new data available for {ticker}")
                        continue

                    # Updating MongoDB Records:
                    new_data.reset_index(inplace=True)
                    new_data_dict = new_data.to_dict("records")

                    # Find the last downloaded date from new_data
                    last_downloaded_date = new_data['Date'].iloc[-1].date()
                    print(f"latest yFinance download date: {last_downloaded_date}")

                    if last_downloaded_date and last_downloaded_date > latest_date:
                        # Insert only the rows that are after the latest_date
                        data_to_insert = [record for record in new_data_dict if record['Date'].date() > latest_date]
                        self.db[ticker].insert_many(data_to_insert)
                        #pp(data_to_insert) #for debugging
                        print(colored(f"--> Success: Data updated for {ticker}",'green'))
                    else:
                        print(colored(f"--> Failed: No new updates for {ticker}",'magenta'))

                except Exception as e:
                    logging.info(f": Exception Triggered: Failed to update data for {ticker}: {str(e)}")
            else:
                print(colored(f"--> No update done: {ticker} already up to date",'yellow'))

class PortfolioManager:
    def __init__(self, portfolio_db, stock_list,main_menu, clear_screen):
        self.portfolio_db = portfolio_db
        self.stock_list = stock_list
        self.main_menu = main_menu
        self.clear_screen = clear_screen

    def manage_portfolio_submenu(self):
        portfolio_options = {
            "1": (self.view_portfolio, "View Portfolio"),
            "2": (self.add_ticker, "Add Ticker"),
            "3": (self.remove_ticker, "Remove Ticker"),
            "4": (self.load_portfolio, "Load from List"),
            "5": (self.main_menu, "Back to Main Menu"),
        }

        while True:
            self.clear_screen()
            print(colored("\n===== Sub-Menu 1: Manage Ticker Portfolio =====",'yellow'))
            for key, value in portfolio_options.items():
                print(f"{key}: {value[1]}")

            select = input(colored("Enter selection number:",'green'))

            if select in portfolio_options:
                try:
                    portfolio_options[select][0]()
                    input(colored("Press Enter to return to the sub-menu...", 'green'))
                except Exception as e:
                    self.status_message = f"Error: {str(e)}"
                    input("Press Enter to return to the sub-menu...")  # Wait for user even in case of error

            else:
                print("--> Invalid Entry")

    def view_portfolio(self):
        print("----- Current Portfolio of Tickers -----")
        for index, ticker in enumerate(sorted(self.stock_list), start=1):
            print(f"{index}) {ticker}")

    def add_ticker(self):
        tickers = input("Enter ticker(s) to add (separate by commas if multiple): ")
        ticker_list = [ticker.strip().upper() for ticker in tickers.split(',') if ticker.strip()]
        if not ticker_list:
            print("--> Invalid ticker input")
            return

        tech_tickers_collection = self.portfolio_db['tech_tickers']

        for ticker in ticker_list:
            existing_ticker = tech_tickers_collection.find_one({'name': ticker})

            if existing_ticker is None:
                print(f"Successfully added {ticker}")
                tech_tickers_collection.insert_one({'name': ticker})
                self.stock_list.append(ticker)

            else:
                print(f"The ticker {ticker} already exists in the database.")

    def remove_ticker(self):
        for ticker in sorted(self.stock_list):
            print(ticker)
        tickers = input("Enter ticker(s) to remove (separate by commas if multiple): ")
        ticker_list = [ticker.strip().upper() for ticker in tickers.split(',')]

        for ticker in ticker_list:
            self.portfolio_db['tech_tickers'].delete_one({'name': ticker})
            if ticker in self.stock_list:
                self.stock_list.remove(ticker)
                print(f"Successfully removed {ticker}")
            else:
                print(f"The ticker {ticker} does not exist in the stock list.")

    def load_portfolio(self):
        portfolio_list = ['AAPL', 'TSLA', 'MSFT']  # replace this with your list
        portfolio_list = [ticker.upper() for ticker in
                          portfolio_list]  # converting to uppercase
        print(f"Current list to be loaded:")
        pp(portfolio_list)
        selector = input(
            "Confirm loading of list (y/n): ").lower()
        if selector == 'y' or selector == 'yes':
            tech_tickers_collection = self.portfolio_db['tech_tickers']

            for ticker in portfolio_list:
                existing_ticker = tech_tickers_collection.find_one({'name': ticker})

                if existing_ticker is None:
                    tech_tickers_collection.insert_one({'name': ticker})
                    self.stock_list.append(ticker)
                    print(f"loaded: {ticker}")
                else:
                    print(f"The ticker {ticker} already exists in the database.")
        elif selector == 'n' or selector == 'no':
            return

class SearchMDB:
    def __init__(self, client, clear_screen):
        self.client = client
        self.clear_screen = clear_screen

    def get_databases(self):
        database_list = self.client.list_database_names()
        user_database_list = [db for db in database_list if db not in ['admin', 'local']]
        return user_database_list

    def get_collections(self, database_name):
        database = self.client[database_name]
        collection_names = database.list_collection_names()
        return collection_names

    def validate_user_selection(self, user_input, max_value):
        try:
            user_input = int(user_input)
            if user_input not in range(1, max_value + 1):
                raise ValueError
        except ValueError:
            print("--> Invalid selection. Please try again.")
            return None
        return user_input

    def get_date_range(self, database_name, collection_name):
        collection = self.client[database_name][collection_name]
        try:
            min_date = collection.find().sort("Date", 1).limit(1)[0]["Date"]
            max_date = collection.find().sort("Date", -1).limit(1)[0]["Date"]
            date_range_str = f"(Date range {min_date.date()} to {max_date.date()})"
        except IndexError:
            date_range_str = "The collection is empty"
        return date_range_str

    def validate_date(self, date_string):
        try:
            if len(date_string) == 4:  # user entered only years
                date = datetime(int(date_string), 1, 1)  # from the beginning of start year
            else:
                date = datetime.strptime(date_string, "%Y-%m-%d")
            return date
        except ValueError:
            print("--> Invalid date format. Please try again.")
            return None

    def query_database(self, selected_collection, start_date, end_date):
        print(f"\n--- Data from {start_date.date()} to {end_date.date()}"
              f" in {selected_collection.name} Collection ---")
        query = {"Date": {"$gte": start_date, "$lt": end_date}}

        # Get the number of documents that match the query
        num_results = selected_collection.count_documents(query)
        print(f"Number of results found: {num_results}")

        cursor = selected_collection.find(query).sort("Date", pymongo.ASCENDING)

        for document in cursor:
            print(document)

    def find_collections(self):
        # Get list of databases
        user_database_list = self.get_databases()
        self.clear_screen()

        for db in user_database_list:
            print(colored(f'\n Found Database: {db}','blue'))

            # Sort the collections before iterating
            sorted_collections = sorted(self.get_collections(db))

            for collection_name in sorted_collections:
                collection = self.client[db][collection_name]
                first_item = collection.find_one() # get the first item

                # If collection is empty, continue to next iteration
                if not first_item:
                    continue

                is_time_series = any(
                    isinstance(value, datetime) for value in first_item.values())
                if is_time_series:
                    print(f'- Time Series Collection: {collection_name}')
                else:
                    print(f'- Non-Time Series Collection: {collection_name}')

        while True:
            print(colored("\n===== Sub-Menu 2: Manage Database Collections =====",'yellow'))
            db_list = self.get_databases()
            for i, db_name in enumerate(db_list):
                print(f"{i + 1}) {db_name}")

            db_select = input(colored("\nSelect database by number or 'b' to return to main menu: ",'green'))
            if db_select.lower() == 'b':
                return
            db_select = self.validate_user_selection(db_select, len(db_list))

            if db_select is None:
                continue

            selected_db = self.client[db_list[db_select - 1]]

            while True:
                print(colored("\n--- Collection Names ---", 'yellow'))
                collection_list = sorted(self.get_collections(db_list[db_select - 1]))
                for i, collection_name in enumerate(collection_list):
                    date_range_str = self.get_date_range(db_list[db_select - 1],
                                                         collection_name)
                    print(f"{i + 1}) {collection_name:<5}\t{date_range_str}") # Padded to 5 Char for uniform alignment

                collection_select = input(colored("\nSelect collection by number or 'b' to go back: ", 'green'))
                if collection_select.lower() == 'b':
                    self.clear_screen()
                    break
                collection_select = self.validate_user_selection(collection_select,
                                                                 len(collection_list))

                if collection_select is None:
                    continue

                selected_collection = self.client[db_list[db_select - 1]][
                    collection_list[collection_select - 1]]

                while True:
                    start_date_input = input(
                        "\nEnter filter start date(YYYY-MM-DD or YYYY) or 'b' to go back: ")
                    if start_date_input.lower() == 'b':
                        self.clear_screen()
                        break
                    end_date_input = input("Enter filter end date (YYYY-MM-DD or YYYY): ")

                    start_date = self.validate_date(start_date_input)
                    end_date = self.validate_date(end_date_input)

                    if start_date is None or end_date is None:
                        continue

                    self.query_database(selected_collection, start_date, end_date)
                    break

# Run the program
if __name__ == "__main__":
    try:
        mongo_initializer = InitEngine()
    except ConnectionError as e:
        logging.error(str(e))