import time
import logging
from configparser import ConfigParser
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os
from datetime import datetime, timedelta
import numpy as np
from halo import Halo
import pandas as pd
import math
import pandas_market_calendars as mcal
import pymongo
from termcolor import colored
from tqdm import tqdm
from pprint import pprint
import pytz
import threading
import sys
from dotenv import load_dotenv

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class InitEngine:
    def __init__(self):
        self.mongodb_config()
        self.connect_mongodb()
        self.appmenu = AppMenu(self.client)

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
        spinner = Halo(text='Connecting to MongoDB: ', spinner='line')
        spinner.start()
        for attempt in range(3):  # Retry up to 3 times
            try:
                self.client.admin.command('ismaster')
                print(f"Successfully Connected as {self.username} to {self.cluster}")
                self.db = self.client['timeseries_db']
                self.portfolio_db = self.client['ticker_db']
                self.stock_list = [ticker['name'] for ticker in self.portfolio_db['tech_tickers'].find()]
                break
            except ConnectionFailure:
                if attempt < 2:  # Only wait if not last attempt
                    time.sleep(2 ** (attempt + 1))
                else:
                    spinner.stop()
                    raise ConnectionError("Unable to establish a connection to the MongoDB server.")
        spinner.stop()

class AppMenu:
    def __init__(self, client):
        self.status_message = ""
        self.client = client
        self.search_mdb = SearchMDB(client)
        self.check_nan = CheckNan(client)
        self.scheduler = Scheduler(self.search_mdb.update_collections_techanalysis)
        self.main_menu()

    def main_menu(self):
        menu_options = {
            "1": (self.ping_database, "Ping MongoDB Database", False),
            "2": (self.search_mdb.init_collection_techanalysis, "Initialize Collections TechAnalysis", True),
            "3": (self.check_nan.main_process, "Check NaN Test", True),
            "4": (self.search_mdb.update_collections_techanalysis, "Update Collections TechAnalysis", True),
            "5": (self.scheduler.start, "Start Scheduled Update for OHLC Database", 0),
            "7": (self.exit_app, "Exit")
        }

        while True:
            self.clear_screen()
            print(colored("\n===== Main Menu Options =====", 'cyan'))
            for key, value in menu_options.items():
                print(f"{key}: {value[1]}")
            print("========== Message Window ==========")
            print(self.status_message)
            self.status_message = ""  # Clear status message
            print("====================================")

            selection = input(colored("Enter selection number: ", 'green'))
            if selection in menu_options:
                try:
                    menu_options[selection][0]()
                    if menu_options[selection][2]:  # Requires confirmation to return
                        input(colored("\nPress Enter to return to the main menu...", 'green'))
                    self.update_status(f"Completed: {menu_options[selection][1]} successfully.")
                except Exception as e:
                    print(f"An error occurred: {str(e)}")
                    self.update_status(f"Last error: {str(e)}")
                    input("\nPress Enter to return to the main menu...")
            else:
                self.update_status("Invalid Entry. Please try again.")

    def ping_database(self):
        self.client.admin.command('ping')
        self.update_status("--> Pinged your deployment. You successfully connected to MongoDB!")

    def clear_screen(self):
        os.system('cls' if os.name == 'nt' else 'clear')

    def update_status(self, message):
        self.status_message = message

    def exit_app(self):
        print("Exiting Program...")
        exit()

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

    """
    def run_scheduled_tasks(self):
        first_run = True
        while True:
            now = pd.Timestamp.now(tz='US/Eastern')
            next_close = self.get_next_market_close()

            # Calculate the time to wait until 10 PM on the day of the market close
            time_at_10pm = next_close.normalize() + pd.Timedelta(hours=22)  # 10 PM on the same day as market close

            if first_run:
                prev_day = now - pd.Timedelta(days=1)
                self.data_updater(scheduler_input=True) #Note that this is unique for dataprocessor engine
                self.data_updated.set()
                print(f"\nFirst run detected: Updating to previous days data ({prev_day.date()})"
                      f"\nScheduling for: {time_at_10pm}")
                first_run = False

            elif now < time_at_10pm:
                print(f"\nNext market closure is at: {next_close}."
                      f"\nScheduling for: {time_at_10pm}")
                self.data_updated.set()

            else:
                self.data_updater(scheduler_input=True) #Note that this is unique for dataprocessor engine
                self.data_updated.set()
                print(f"\nDaily Scheduler Active: Scheduling for {time_at_10pm}.")

            # Wait until after running update operation
            sleep_seconds = (time_at_10pm - now).total_seconds()
            time.sleep(max(sleep_seconds, 0))
    """
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
                self.data_updater(scheduler_input=True)
                self.data_updated.set()
                print(f"\nFirst run detected: Updating to previous days data ({prev_day.date()})"
                      f"\nScheduling for: {time_at_10pm}")
                first_run = False

            elif now >= prev_close and now <= next_close and now >= prev_time_at_10pm:
                print(f"\nMarket has closed on {prev_close}."
                      f"\nRunning Updater... "
                      f"\nDaily Scheduler Active for: {time_at_10pm}")
                self.data_updater(scheduler_input=True)
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

class DateInputHelper:
    @staticmethod
    def get_input_date(prompt_message):
        """Get input date with specific prompt message."""
        while True:
            date_str = input(prompt_message)
            if len(date_str) == 4:  # Only year is given
                try:
                    start_date = datetime.strptime(date_str, '%Y')
                    end_date = start_date.replace(month=12, day=31)
                    return start_date, end_date
                except ValueError:
                    print("Invalid year format. Please retry.")
            else:  # Full date is given
                try:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    return date_obj, None
                except ValueError:
                    print(
                        "Invalid date format. Accepted format is 'yyyy-mm-dd'. Please retry.")

    @staticmethod
    def get_chronological_dates():
        """Get start and end dates ensuring the start date is before the end date."""
        start_date, _ = DateInputHelper.get_input_date(
            "Please enter the start date (in 'yyyy-mm-dd' or 'YYYY' format): ")
        end_date, _ = DateInputHelper.get_input_date(
            "Please enter the end date (in 'yyyy-mm-dd' or 'YYYY' format): ")

        while end_date and end_date < start_date:
            print("End date must be after the start date. Please re-enter.")
            end_date, _ = DateInputHelper.get_input_date(
                "Please enter the end date (in 'yyyy-mm-dd' format): ")
        return start_date, end_date

class SearchMDB:
    def __init__(self, client):
        self.client = client
        self.scheduler_input = False

    def get_databases(self):
        database_list = self.client.list_database_names()
        user_database_list = [db for db in database_list if db not in ['admin', 'local']]
        print(user_database_list)
        return user_database_list

    def _get_date(self, collection, order=1):
        # Retrieve the date from a collection in the specified order.
        date_entry = collection.find().sort("Date", order).limit(1)
        try:
            return date_entry.next()['Date'].strftime('%Y-%m-%d')
        except StopIteration:
            return 'N/A'

    def _working_days_from_date(self, date, days):
        nyse = mcal.get_calendar('NYSE')
        valid_days = nyse.valid_days(start_date=date - timedelta(days=365), end_date=date)
        adjusted_date = valid_days[-days - 1]
        return adjusted_date.to_pydatetime()

    def _run_ta_calculations(self, collection, start_date, end_date):
        cursor = collection.find({"Date": {"$gte": start_date, "$lte": end_date}})
        df = pd.DataFrame(list(cursor))
        df = self.Bollinger_Bands(df)
        df = self.Mavgs(df)
        return df

    def Bollinger_Bands(self, data):
        mov_window = 21
        MA = data['Close'].rolling(window=mov_window).mean()
        SD = data['Close'].rolling(window=mov_window).std()
        data['Lower_BB'] = MA - (2 * SD)
        data['Upper_BB'] = MA + (2 * SD)
        return data

    def Mavgs(self, saved_data):
        short_window = 50
        med_window = 100
        long_window = 200

        saved_data['short_mavg'] = saved_data['Close'].rolling(short_window).mean()
        saved_data['med_mavg'] = saved_data['Close'].rolling(med_window).mean()
        saved_data['long_mavg'] = saved_data['Close'].rolling(long_window).mean()

        saved_data['long_positions'] = np.where(
            saved_data['short_mavg'] > saved_data['long_mavg'], 1, 0)
        saved_data['short_positions'] = np.where(
            saved_data['short_mavg'] < saved_data['long_mavg'], -1, 0)
        saved_data['positions'] = saved_data['long_positions'] + saved_data['short_positions']
        return saved_data

    def update_collection_with_positions(self, df, collection_name):
        database = self.client['timeseries_db']
        collection = database[collection_name]

        for index, row in tqdm(df.iterrows()):
            collection.update_one(
                {"_id": row['_id']},
                {
                    "$set": {
                        "Lower_BB": row['Lower_BB'],
                        "Upper_BB": row['Upper_BB'],
                        "short_mavg": row['short_mavg'],
                        "med_mavg": row['med_mavg'],
                        "long_mavg": row['long_mavg'],
                        "long_positions": row['long_positions'],
                        "short_positions": row['short_positions'],
                        "positions": row['positions']
                    }
                }
            )

    def choose_collection_for_db(self):
        database = self.client['timeseries_db']
        collections = sorted(database.list_collection_names())
        print("\nPlease choose a collection:")

        for idx, collection_name in enumerate(collections, 1):
            collection = database[collection_name]
            first_date = self._get_date(collection, 1)
            last_date = self._get_date(collection, -1)
            print(f"{idx}. {collection_name}: ({first_date}-{last_date})")

        if self.scheduler_input:
            return collections, database
        else:
            choice = input(f"Enter a number, 'all' for all collections or 'b' to go back: ")

            if choice.lower() == 'all':
                return collections, database
            elif choice.lower() == 'b':
                return None
            elif choice.isdigit() and 1 <= int(choice) <= len(collections):
                chosen_collections = [collections[int(choice) - 1]]
                return chosen_collections, database
            else:
                print("--> Invalid input, enter value again: ")
                return self.choose_collection_for_db()  # Prompt the user again

    def init_collection_techanalysis(self):
        result = self.choose_collection_for_db()

        # Check if the result is None (i.e., user chose to go back)
        if result is None:
            print("--> Returning to Main Menu")
            return  # Exit the function to prevent further execution

        collection_names, database = self.choose_collection_for_db()
        print(collection_names)
        start_date, end_date = DateInputHelper.get_chronological_dates()
        print(
            f"Calculation Window: {start_date.strftime('%d-%m-%y')} - {end_date.strftime('%d-%m-%y')}")

        for collection_name in collection_names:
            collection = database[collection_name]
            df = self._run_ta_calculations(collection, start_date, end_date)

            # Check and filter out NaN values from the DataFrame
            # You can adjust columns in `nan_columns` to fit your specific needs
            nan_columns = ['Lower_BB', 'Upper_BB', 'short_mavg', 'med_mavg', 'long_mavg']
            df.dropna(subset=nan_columns, inplace=True)

            # Ensure there is data left to update after removing NaNs
            if not df.empty:
                # Update the documents with the processed data
                self.update_collection_with_positions(df, collection_name)
                print(f"Updated {collection.name} with non-NaN entries.")
            else:
                print(f"No data to update for {collection.name} due to NaN filtering.")

            # Optionally, you can show the DataFrame (or part of it) to verify the updates
            print(colored(f"\n----- {collection_name} -----","cyan"))
            print(df.head())  # Shows the first few rows of the DataFrame

        return collection_names

    def update_collections_techanalysis(self, scheduler_input=False):
        self.scheduler_input = scheduler_input
        result = self.choose_collection_for_db()

        if result is None:
            print("--> Returning to Main Menu")
            return

        chosen_collections, database = result

        if not chosen_collections:
            print("Exiting update process.")
            return

        for collection_name in chosen_collections:
            collection = database[collection_name]
            first_doc_with_long_mavg = collection.find_one({"long_mavg": {"$exists": True}}, sort=[("Date", pymongo.ASCENDING)])
            last_doc_with_long_mavg = collection.find_one({"long_mavg": {"$exists": True}}, sort=[("Date", pymongo.DESCENDING)])

            if not first_doc_with_long_mavg:
                print(f"{[collection_name]} Error: The collection will need to be initialized first.")
                continue

            start_date = last_doc_with_long_mavg["Date"]
            adjusted_start_date = self._working_days_from_date(start_date, 200)
            end_date = datetime.now()

            print(f"Processing data for {collection_name} from {adjusted_start_date.strftime('%d-%m-%y')} to {end_date.strftime('%d-%m-%y')}")

            df = self._run_ta_calculations(collection, adjusted_start_date, end_date)

            # Filter the dataframe to only include new records
            df_to_update = df[df['Date'] > start_date]

            # Update the documents with the processed data
            self.update_collection_with_positions(df_to_update, collection_name)

            print(colored(f"\n----- {collection_name} -----","blue"))
            print(df_to_update)

class CheckNan:
    def __init__(self, client):
        self.client = client

    def get_databases(self):
        database_list = self.client.list_database_names()
        user_database_list = [db for db in database_list if db not in ['admin', 'local']]
        return user_database_list

    def get_collections(self, database_name):
        database = self.client[database_name]
        collection_names = database.list_collection_names()
        return collection_names

    def choose_collection(self):
        """Provide the user a submenu to choose a collection"""
        db_name = "timeseries_db"  # Assuming you always want to work in this DB.
        database = self.client[db_name]
        collections = sorted(database.list_collection_names())

        def get_date(collection, order=1):
            """Retrieve the date from a collection in the specified order.
               Order 1 for the first date and -1 for the last date.
            """
            date_entry = collection.find().sort("Date", order).limit(1)
            try:
                return date_entry.next()['Date'].strftime('%Y-%m-%d')
            except StopIteration:
                return 'N/A'

        print("Please choose a collection:")
        for idx, collection_name in enumerate(collections, 1):
            collection = database[collection_name]

            first_date = get_date(collection, 1)
            last_date = get_date(collection, -1)

            print(f"{idx}. {collection_name}: ({first_date}-{last_date})")

        choice = input(f"Enter a number, 'all' for all collections or 'b' to go back: ")

        if choice.lower() == 'all':
            return collections
        elif choice.lower() == 'b':
            return None  # You can return None to indicate "back" to the calling function
        elif choice.isdigit() and 1 <= int(choice) <= len(collections):
            return collections[int(choice) - 1]
        else:
            print("Invalid input!")
            return self.choose_collection()  # Prompt the user again

    def main_process(self):
        chosen_collections = self.choose_collection()

        if chosen_collections is None:
            print("--> Returning to Main Menu")
            return

        if not isinstance(chosen_collections, list):
                chosen_collections = [chosen_collections]

        # Use the DateInputHelper class to retrieve dates
        start_date, end_date = DateInputHelper.get_chronological_dates()

        for item in chosen_collections:
            # Connect to the collection
            collection = self.client['timeseries_db'][item]

            # Normal Check
            print(colored(f"\n----- {item} -----","cyan"))
            cursor = collection.find({"Date": {"$gte": start_date, "$lte": end_date}})
            cursor_list = list(cursor)
            print(f"Records Found:  {len(cursor_list)}")
            if cursor_list:  # checks if the list is not empty
                print("\t=== First Entry ===")
                for key, value in cursor_list[0].items():
                    print(f"{key:12}\t: {value}")
                # If there's more than one record, print the last one
                if len(cursor_list) > 1:
                    print("\t=== Last Entry ===")
                    for key, value in cursor_list[-1].items():
                        print(f"{key:12}\t: {value}")

            if len(cursor_list) == 0:
                print(
                    f"No records found for collection {collection.name} within the provided date range.")

            # NaN Check
            print("\t=== NaN Check ===")
            cursor = collection.find({
                "Date": {"$gte": start_date, "$lte": end_date},
                "long_mavg": {"$type": "double"}
            })
            cursor_list = list(cursor)
            nan_records = [record for record in cursor_list if math.isnan(record["long_mavg"])]
            if nan_records:  # checks if the list is not empty
                # Print first record
                print(f"First Entry:{nan_records[0]}")

                # If there's more than one record, print the last one
                if len(nan_records) > 1:
                    print(f"Last Entry:{nan_records[-1]}")

            if len(nan_records) == 0:
                print(f"{len(nan_records)} NaN records found for {collection.name} within date range.")
            else:
                print(colored(f"{len(nan_records)} NaN records found for {collection.name} between {start_date} - {end_date}.","red"))


# Run the program
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        data_processor = InitEngine()
    except ConnectionError as e:
        logging.error(str(e))
