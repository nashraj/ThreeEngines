import logging
import os
from configparser import ConfigParser
import pymongo
from pymongo import MongoClient, errors as pymongo_errors
import pandas_market_calendars as mcal
import pandas as pd
from datetime import datetime, timedelta
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.requests import TrailingStopOrderRequest
from termcolor import colored
from pprint import pp
import numpy as np
import subprocess
from tabulate import tabulate
import psutil
import threading
import time
import sys
from tqdm import tqdm
from dotenv import load_dotenv

"""
### Important Modules to add
1) If meant to avoid flagging:
    - Add tracking of day trading flagging (4x pair-directional trades per 5 working days)
2) If to be tracked as Day Trading Account:
    - $25k minimum balance liquidity or 20% of allocated capital (verify again in alpaca)
3) def calculate_lot_size:
    - Oversight is that the alloted amount is a static shot at point of calculation not dynamic at this stage
4) Fix trading hours and calander days
"""

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class InitEngine:
    def __init__(self):
        self.status_message = ""
        self.mongodb_config()
        self.connect_mongodb()
        self.trading_client = self.init_alpaca_connection()
        self.launch_trading_stream()
        self.check_market = CheckMarket(self.client)
        self.trade_decision = TradeDecision(self.trading_client, self.stock_list, self.db)
        self.market_controller = MarketController(self.client, self.stock_list, self.db, self.trade_decision)
        self.scheduler = Scheduler(self.market_controller)
        self.main_menu()

    def mongodb_config(self):
        load_dotenv('mongodbconfig.env')
        self.username = os.getenv('MONGODB_USERNAME')
        password = os.getenv('MONGODB_PASSWORD')
        self.cluster = os.getenv('MONGODB_CLUSTER')
        cluster_uri = f"mongodb+srv://{self.username}:{password}@{self.cluster}.zcka1qs.mongodb.net/?retryWrites=true&w=majority"
        self.connection_options = {
            "connectTimeoutMS": 5000,
            "socketTimeoutMS": 5000,
            "retryWrites": True,
            "retryReads": True,
            "w": "majority"
        }
        self.client = MongoClient(cluster_uri, serverSelectionTimeoutMS=10000, **self.connection_options)
        self.db = self.client['timeseries_db']
        self.portfolio_db = self.client['ticker_db']
        self.stock_list = [ticker['name'] for ticker in self.portfolio_db['tech_tickers'].find()]

    def connect_mongodb(self):
        try:
            self.client.admin.command('ismaster')
            print(f"Successfully Connected as {self.username} to {self.cluster}")
        except pymongo_errors.ConnectionFailure as e:
            raise ConnectionError(f"Unable to establish a connection to the MongoDB server: {str(e)}")

    def init_alpaca_connection(self):
        load_dotenv('alpacaconfig.env')
        API_KEY = os.getenv('API_KEY')
        SECRET_KEY = os.getenv('SECRET_KEY')
        return TradingClient(API_KEY, SECRET_KEY, paper=True)

    def launch_trading_stream(self):
        # Check if the trading stream is already running
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                # Ensure cmdline is not None and contains the script name
                if proc.info['cmdline'] and "v0.4) trading_stream_script.py" in ' '.join(proc.info['cmdline']):
                    print("Trading stream already running.")
                    return
            except Exception as e:
                print(f"Error checking process: {e}")
                continue  # Skip to the next process

        # Prepare the command to run the trading stream script in a separate window
        file = "v0.4) trading_stream_script.py"
        command = ['start', 'cmd', '/c', 'python', file]

        # Convert the command list to a properly escaped command string
        command_str = subprocess.list2cmdline(command)

        # Launch the trading stream script in a separate window
        subprocess.run(command_str, shell=True)

    def main_menu(self):
        menu_options = {
            "1": (self.ping_database, "Ping MongoDB Database",0),
            "2": (self.market_controller.check_and_trade, "Run Order Management System", 1),
            "3": (self.alpaca_account, "Alpaca Account Details", 1),
            "4": (self.exit_app, "Exit", 0),
            "5": (self.scheduler.start, "Start Automated Daily Run", 0)
        }
        while True:
            self.clear_screen()
            print(colored("============ Main Menu =============", "yellow"))
            for key, value in menu_options.items():
                print(f"{key}: {value[1]}")
            print("========== Message Window ==========")
            print(f"{self.status_message}")
            self.status_message = ""  # Clear status message after displaying
            print("====================================")

            user_input = input(colored("Enter selection number: ","green"))

            if user_input in menu_options:
                try:
                    menu_options[user_input][0]()  # Execute the selected option
                    self.status_message = f"Successfully ran: {menu_options[user_input][1]}"
                    if menu_options[user_input][2] == 1:
                        input(colored("\nPress Enter to return to the main menu...",
                                      'green'))  # Wait for user to review output and confirm
                except Exception as e:
                    self.status_message = f"Error: {str(e)}"
                    input(colored("Press Enter to return to the main menu...", "red"))
            else:
                self.status_message = "--> Invalid Entry"

    def ping_database(self):
        try:
            self.client.admin.command('ping')
            self.status_message = "--> Ping successful!"
        except pymongo_errors.PyMongoError as e:
            self.status_message = f"Error during ping: {str(e)}"

    def clear_screen(self):
        import os
        os.system('cls' if os.name == 'nt' else 'clear')

    def alpaca_account(self):
        self.account_info = self.trading_client.get_account()
        print(tabulate(self.account_info, headers=["Alpaca Account"], tablefmt="simple_outline"))

    def exit_app(self):
        print("Exiting Program...")
        exit()

class Scheduler:
    def __init__(self, market_controller):
        self.market_controller = market_controller
        self.trade_completed = threading.Event()
        self.timer_active = False
        self.timer_lock = threading.Lock()

    def get_next_market_open(self):
        # Get the NYSE calendar
        nyse = mcal.get_calendar('NYSE')

        # Get today's date and calculate the end date for the range
        now = pd.Timestamp.now(tz='US/Eastern')  # Ensure timezone alignment with NYSE
        end_date = now + pd.Timedelta(days=10)  # Check the next 10 days to ensure we cover weekends and holidays

        # Get the trading schedule for the given date range
        schedule = nyse.schedule(start_date=now.date().isoformat(), end_date=end_date.date().isoformat())

        # Find the next market open time after now
        next_open_times = schedule[schedule.market_open > now]['market_open']
        if not next_open_times.empty:
            next_open = next_open_times.iloc[0]
            return next_open.tz_convert('US/Eastern')
        else:
            # Handle the case where no market open time is found within the range
            print("No market open times found in the specified range.")
            return now + pd.Timedelta(days=1)  # Default to the next day, adjust as needed

    def run_scheduled_tasks(self):
        first_run = True
        while True:
            now = pd.Timestamp.now(tz='US/Eastern')

            if first_run:
                nasdaq = mcal.get_calendar('NASDAQ')
                current_date = pd.Timestamp(datetime.now())
                market_open = nasdaq.valid_days(start_date=current_date, end_date=current_date).shape[0] > 0
                next_run = self.get_next_market_open()

                if market_open:
                    # If now is during or after the market open, run immediately and find the next market open
                    print(f"\nClass Scheduler: Detected market will be open on {next_run}: scheduling trades...")
                    self.market_controller.check_and_trade()
                    self.trade_completed.set()
                    next_run = self.get_next_market_open()
                    first_run = False
                elif now <= next_run:
                    # If now is before the market open, wait until the market opens
                    print(f"\nClass Scheduler: Detected market is closed. OMS scheduled to run upon market opening on {next_run}.")
                    sleep_seconds = (next_run - now).total_seconds()
                    time.sleep(max(sleep_seconds, 0))
                    first_run = False
            else:
                self.market_controller.check_and_trade()
                self.trade_completed.set()
                next_run = self.get_next_market_open()
                print(f"\nClass Scheduler: Daily scheduler active, scheduling run for market upon opening on {next_run}.")

            # Wait until the next market open, after running trading operations
            sleep_seconds = (next_run - now).total_seconds()
            time.sleep(max(sleep_seconds, 0))

    def display_timer(self):
        while True:
            self.trade_completed.wait()  # Wait for the trade_completed event to be set
            next_run = self.get_next_market_open()
            while self.timer_active:
                with self.timer_lock:
                    now = pd.Timestamp.now(tz='America/New_York')
                    remaining = int((next_run - now).total_seconds())
                    # remaining = 10 # Debugging
                    if remaining > 0:
                        sys.stdout.write(
                            f"\rTime until next run: {remaining // 3600} hours, "
                            f"{(remaining % 3600) // 60} minutes, "
                            f"{(remaining % 60)} seconds ")
                        sys.stdout.flush()
                    else:
                        break
                    time.sleep(1)
            sys.stdout.write("\rTime until next run: ready!          \n")
            self.trade_completed.clear()  # Reset event for the next cycle

    def start(self):
        self.timer_active = True
        scheduler_thread = threading.Thread(target=self.run_scheduled_tasks)
        scheduler_thread.daemon = True
        scheduler_thread.start()

        timer_thread = threading.Thread(target=self.display_timer)
        timer_thread.daemon = True
        timer_thread.start()

class CheckMarket:
    def __init__(self, client):
        self.client = client

    def check_market_status(self):
        nasdaq = mcal.get_calendar('NASDAQ')
        current_date = pd.Timestamp(datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d")
        market_open = nasdaq.valid_days(start_date=current_date, end_date=current_date).shape[0] > 0
        if not market_open:
            print(f"Market is closed on {current_date}.")
            return False
        return True

class TradeDecision:
    def __init__(self, trading_client, stock_list, db):
        self.trading_client = trading_client
        self.stock_list = stock_list
        self.account_info = self.trading_client.get_account()
        self.db = db
        self.trade_log = {}

    def handle_trading_decision(self, symbol, ):

        # Get Ticker from MongoDB Collection:
        print(colored(f"\n----- {symbol} -----","blue"))
        latest_data = self.get_latest_trading_data(symbol)
        #pp(latest_data) #Debugging
        print(tabulate(list(latest_data.items()), headers=[symbol, "Latest MongoDB Collection"], tablefmt="simple_outline"))
        if not latest_data:
            logging.info(f"No trading data to process for {symbol}")
            return

        # Check for existing positions/quantity:
        cost_basis, current_qty, side, market_value, lastday_price= self.get_current_qty(symbol)
        data = [
            ("Position Entry Cost", f"${cost_basis:.2f}"),
            ("Direction", side.name.lower() if side else "None"),
            ("Current quantity", current_qty),
            ("Last day price", f"${lastday_price:.2f}"),
            ("Market Value", f"${market_value:.2f}")
        ]
        print(tabulate(data, headers=[symbol, "Existing Position"], tablefmt="simple_outline"))

        #Evaluate strategy positional data:
        # Impt: refer to initialize_alpaca_position for alpaca initialization strategy
        long_positions = latest_data['long_positions']
        short_positions = latest_data['short_positions']

        # Prevent Critical Error in positional data:
        if long_positions == 1 and short_positions == -1:
            logging.info(f"Critical Error in Strategy: Simultaneous Buy and Sell Detected."
                         f"/n Halting Order Management System...")
            exit()

        # Lot size currently evenly splits to number of active tickers in portfolio
        # and avoids buying in more / selling in more if available funds < existing position
        current_close = self.db[symbol].find_one(sort=[("Date", -1)])
        sub_portion = self.calculate_lot_size()
        lot_size = np.floor(sub_portion / current_close['Close'])

        # Combination Stop loss strategy using bollingerband delta and ATR
        stop_loss, take_profit = self.calculate_risk_parameters(latest_data, symbol)

        # Strategy Verification and Risk Plan Summary:
        data = [
            ("Strategy long positions", f"{long_positions}"),
            ("Strategy short positions", f"{short_positions}"),
            ("Total Cash Available", f"${self.account_info.cash}"),
            ("Portfolio Tickers", f"{len(self.stock_list)}"),
            ("Cash Allotment", f"${sub_portion:.2f}"),
            ("Lot Allotment", f"{float(lot_size):.2f}"),
            ("Stop loss", f"{stop_loss:.2f}%"),
            ("Take profit", f"{take_profit:.2f}%")
        ]
        print(tabulate(data, headers=[symbol, "Strategy"], tablefmt="simple_outline"))

        # Trade Decision Algorithm:
        # Important note: Position Pairs will trigger flagging of Day-Trading Account status
        # if 4 full pair trades occur in 5 working days

        # Position Check against available funds:
        # ---> Tentatively not buying/selling more into the position yet (to improve at later stage)
        # ---> Can consider using alpaca bracketing orders
        """Important note about 'side' variable: It is a member of an enumerate class with .name & .value"""
        if float(cost_basis) >= float(lot_size):
            if side == "long" and long_positions == 1 and short_positions == 0:
                print(colored(f"{symbol} {side.name} position full based on available allotted funds, skipping ticker","red"))
                self.trade_log.update({symbol:f"{side.name:5} position full, no changes made"})

            elif side == "short" and long_positions == 0 and short_positions == -1:
                print(colored(f"{symbol} {side.name} position full based on available allotted funds, skipping ticker","red"))
                self.trade_log.update({symbol:f"{side.name:5} position full, no changes made"})

        # Position Initialization Check (when no positions):
        if long_positions and not current_qty:
            print(colored(f"Buy-Initialize strategy detected","green"))
            self.enter_trade(symbol, lot_size, OrderSide.BUY, stop_loss, take_profit)
            self.trade_log.update({symbol:f"Buy-Initialize strategy deployed"})
        elif short_positions and not current_qty:
            print(colored(f"Sell-Initialize strategy detected","yellow"))
            self.enter_trade(symbol, lot_size, OrderSide.SELL, stop_loss, take_profit)
            self.trade_log.update({symbol:f"Sell-Initialize strategy deployed"})

        # Existing to No Position Check:
        if side == "long" and long_positions == 0 and short_positions == 0:
            print(f"Exit Point detected for current {side.name} for {current_qty} at {cost_basis} initial cost"
                  f"\nLiquidating of position will occur at: {market_value}")
            self.exit_trade(symbol, current_qty, OrderSide.SELL)
            self.trade_log.update({symbol:f"{side.name} exit strategy deployed"})
        if side == "short" and long_positions == 0 and short_positions == 0:
            print(f"Exit Point detected for current {side.name} for {current_qty} at {cost_basis} initial cost"
                  f"\nLiquidating of position will occur at: {market_value}")
            self.exit_trade(symbol, current_qty, OrderSide.BUY)
            self.trade_log.update({symbol:f"{side.name} exit strategy deployed"})

        # Existing to Flip Position Check:
        if side == "long" and long_positions == 0 and short_positions == -1:
            print(f"Sell-Flip strategy detected for current {side.name} for {current_qty} at {cost_basis} initial cost"
                  f"\nLiquidating of position will occur at: {market_value}")
            self.exit_trade(symbol, current_qty, OrderSide.SELL)
            self.enter_trade(symbol, self.adj_qty(market_value,lastday_price), OrderSide.SELL, stop_loss, take_profit)
            self.trade_log.update({symbol:f"{side.name} flip strategy deployed"})

        elif side == "short" and long_positions == 1 and short_positions == 0:
            print(f"Buy-Flip strategy detected for current {side.name} for {current_qty} at {cost_basis} initial cost"
                  f"\nLiquidating of position will occur at: {market_value}")
            self.exit_trade(symbol, current_qty, OrderSide.BUY)
            self.enter_trade(symbol, self.adj_qty(market_value,lastday_price), OrderSide.BUY, stop_loss, take_profit)
            self.trade_log.update({symbol:f"{side.name} flip strategy deployed"})

    def get_latest_trading_data(self, symbol):
        """
        collection = self.trading_client.data_db[symbol]
        return collection.find_one(sort=[('Date', pymongo.DESCENDING)])
        """
        latest_doc = self.db[symbol].find_one(sort=[("Date", -1)])
        return latest_doc

    def get_current_qty(self, symbol):
        try:
            position = self.trading_client.get_open_position(symbol)
            # --- Debugging ---
            #pp(position)
            #headers = [symbol, "Existing Position"]
            #print(tabulate(position, headers=headers, tablefmt="simple_outline"))
            # --- --------- ---
            return (abs(float(position.cost_basis)), float(position.qty), position.side,
                    abs(float(position.market_value)), float(position.lastday_price))
        except Exception as e:
            print(colored(f"No Position found for {symbol}: {str(e)}","red"))
            return 0, 0, None, 0, 0

    def adj_qty(self, market_value, lastday_price):
        adj_qty = np.floor(float(market_value) / float(lastday_price))
        return adj_qty

    def calculate_lot_size(self):
        """ Allocate available cash equally across all tickers in the stock list."""
        if len(self.stock_list) > 0:  # Ensure there are tickers in the list to prevent division by zero
            sub_portion = float(self.account_info.cash) / len(self.stock_list)
        else:
            logging.error("No tickers found in the stock list.")
            sub_portion = 0  # Handle the case when there are no tickers
        return sub_portion

    def client_order_id(self, symbol, side):
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        client_order_id = f"{symbol}-{side.value}-{timestamp}"
        return client_order_id

    def calculate_bb_stoploss(self, data):
        lower_bb = data['Lower_BB']
        upper_bb = data['Upper_BB']
        band_width = upper_bb - lower_bb
        bb_stop_loss = band_width / lower_bb
        bb_take_profit = bb_stop_loss * 2
        #print(bb_stop_loss, bb_take_profit)
        return bb_stop_loss, bb_take_profit

    def calculate_atr_stoploss(self, symbol, window=14):
        """ Calculate the Average True Range (ATR) for a stock. """
        recent_docs = list(self.db[symbol].find().sort("Date", -1).limit(window)) # list of dictionaries
        df = pd.DataFrame(recent_docs)  #convert LOD to dataframe
        current_close = self.db[symbol].find_one(sort=[("Date", -1)])
        #print(df) #Debugging
        #print(current_close['Close']) #Debugging

        # Ensure dates are sorted (just in case)
        df.sort_values('Date', inplace=True)

        # Calculate components of True Range
        high_low = df['High'] - df['Low']
        high_close = (df['High'] - df['Close'].shift()).abs()
        low_close = (df['Low'] - df['Close'].shift()).abs()

        # Combine the components into a single DataFrame
        ranges = pd.concat([high_low, high_close, low_close], axis=1)

        # Calculate the true range
        true_range = ranges.max(axis=1)

        # Calculate the ATR
        atr = true_range.rolling(window=14).mean()  # assuming you want a rolling ATR over 14 periods
        atr_value = float(atr.iloc[-1])
        #print(f"The ATR value for {symbol} is:", atr_value) debugging

        #calculate ATR SL/TP
        atr_stop_loss = 1 - ((current_close['Close'] - (1.5 * atr_value)) / current_close['Close']) # Stop loss at 1.5x ATR below current price
        atr_take_profit = ((current_close['Close'] + atr_value * 3) / current_close['Close']) - 1 # Take profit at 3x ATR below current price
        #print(atr_stop_loss, atr_take_profit)

        return atr_stop_loss, atr_take_profit

    def calculate_risk_parameters(self, latest_data, symbol):
        bb_stop_loss, bb_take_profit = self.calculate_bb_stoploss(latest_data)
        atr_stop_loss, atr_take_profit = self.calculate_atr_stoploss(symbol)
        #print(bb_stop_loss, bb_take_profit,atr_stop_loss, atr_take_profit) #debugging

        # Combine or choose strategy
        stop_loss = min(bb_stop_loss, atr_stop_loss)
        if stop_loss <= 0.1:
            stop_loss = 0.1
        take_profit = max(bb_take_profit, atr_take_profit)
        return stop_loss, take_profit

    def check_existing_order(self, client_order_id):
        existing_orders = self.trading_client.get_orders()
        #pp(existing_orders) # debugging
        try:
            # Assume client_order_id format is "SYMBOL-SIDE-YYYYMMDDHHMMSS"
            target_timestamp_str = client_order_id.split('-')[-1]
            target_date = datetime.strptime(target_timestamp_str, "%Y%m%d%H%M%S").date()

            for order in existing_orders:
                existing_timestamp_str = order.client_order_id.split('-')[-1]
                existing_date = datetime.strptime(existing_timestamp_str, "%Y%m%d%H%M%S").date()

                # Compare only the date parts
                if existing_date == target_date:
                    logging.info(f"Duplicate order attempt detected for date: {target_date.strftime('%Y-%m-%d')}")
                    return True
        except ValueError as e:
            logging.error(f"Error parsing dates from client_order_id: {str(e)}")
            return False

        return False

    def countdown_timer(self, seconds):
        for i in tqdm(range(seconds, 0, -1), desc="Processing", unit="second", leave=True):
            time.sleep(1)

    def enter_trade(self, symbol, qty, side, stop_loss, take_profit):
        """ Enter a trade with a dynamic stop-loss and order duplicate check. """

        order_id = self.client_order_id(symbol, side)
        if self.check_existing_order(order_id):
            print(f"Order {order_id} is a duplicate and will not be placed.")
            return

        order_data = TrailingStopOrderRequest(
            symbol=symbol,
            qty=qty,
            side=side,
            time_in_force=TimeInForce.DAY,
            trail_percent = stop_loss,
            client_order_id = order_id,
        )
        print(tabulate(order_data, headers=["Enter Order"], tablefmt = "psql"))
        self.trading_client.submit_order(order_data)
        self.countdown_timer(10)

    def exit_trade(self, symbol, qty, side):
        """ Market Order Exit Trade and order duplicate check. """

        order_id = self.client_order_id(symbol, side)
        if self.check_existing_order(order_id):
            print(f"Order {order_id} is a duplicate and will not be placed.")
            return

        order_data = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=side,
            time_in_force=TimeInForce.DAY,
            client_order_id = self.client_order_id(symbol,side),
        )
        print(tabulate(order_data, headers=["Exit Order"], tablefmt = "psql"))
        self.trading_client.submit_order(order_data)
        self.countdown_timer(10)

class MarketController:
    def __init__(self, client, stock_list, db, trade_decision):
        self.client = client
        self.trade_decision = trade_decision
        self.stock_list = stock_list
        self.db = db

    def check_and_trade(self):
        check_market = CheckMarket(self.client)
        if check_market.check_market_status():
            print("Class MarketController: Market is open. Proceeding with trading decisions...")
            self.process_trades()
        else:
            print("Class MarketController: Market is closed or not ready. Halting operations.")

    def process_trades(self):
        print(self.stock_list)
        for symbol in self.stock_list: #[0:1]:
            self.trade_decision.handle_trading_decision(symbol)
        print(tabulate(self.trade_decision.trade_log.items(), headers=['Ticker', 'Operation'], stralign="left"))

if __name__ == "__main__":
    try:
        init_engine = InitEngine()
    except ConnectionError as e:
        logging.error(f"Failed to start OMS: {str(e)}")
