# 3.1) trading_stream_script.py
from alpaca.trading.stream import TradingStream
import time
from pprint import pp

API_KEY = "PK5BIROFCO2SEJMEJ4RV"
SECRET_KEY = "Gbvdx9YPtFdGt8QRdvwEwACbjbuhFa8Lbs75N6Gy"
trading_stream = TradingStream(API_KEY, SECRET_KEY, paper=True)

async def update_handler(data):
    # trade updates will arrive in our async handler
    print(f"\n----- Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')} -----")
    pp(data)

# subscribe to trade updates and supply the handler as a parameter
trading_stream.subscribe_trade_updates(update_handler)

# start our websocket streaming
print("--- Trade Stream Running ---")
trading_stream.run()
