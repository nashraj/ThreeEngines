![Intel Blowout avoided edit 2024-08-22 015154](https://github.com/user-attachments/assets/46fdd4a2-329d-4dc5-9488-c6c0b0f52f56)
![2024-08-21-171717_1920x1080_scrot](https://github.com/user-attachments/assets/f2e1fa8c-7e98-4080-a70e-62868392e5de)

# Three Engines Project - Automated Trading Engine

## Overview
This project consists of three modules that work together to manage and execute trading strategies using market data from Yahoo Finance, process the data, and place orders through the Alpaca broker interface with a CLI control over portfolio selection and MongoDB database management.

## Components
### 1. Yahoo Finance MongoDB Interface (YFinance MongoDB 204 MVP.py)
**Description:**
  - This engine handles fetching market data from Yahoo Finance and storing it in a MongoDB database.
**Features:**
  - CLI interface for portfolio management (manual or csv input)
  - Automatically Fetches market data using Yahoo Finance API with market calender scheduler function.
  - Stores fetched data in MongoDB and updates the database with incremental changes to minimize load on yFinance API.
**Usage:**
  - To start the Yahoo Finance MongoDB Interface, run the `YFinanceMongoDB.py` script. Make sure your MongoDB connection parameters are set in the environment variables and run in PyCharm with emulated terminal environment for best experince.

### 2. Data Processor (data processor 204 MVP.py)
**Description:** 
  - This engine processes market data to generate trading signals using Bolingerbands and Moving averages with error checking functionality
    
**Features:**
  - Uses stored market data from the MongoDB database.
  - Applies proprietary trading strategy algorithms.
  - Stores trading signals in the MongoDB Database
  - Scheduler function integrated to work with Yahoo Finance MongoDB Interface
**Usage:**
  - To start the Data Processor, run the `DataProcessor.py` script. The script will fetch data from the MongoDB database, process it, and send signals based on the defined strategy. Make sure your MongoDB connection parameters are set in the environment variables and run in PyCharm with emulated terminal environment for best experince.
  
### 3. Alpaca Order Management System (Alpaca OMS MK5 059 MVP.py, trading_stream_script.py)
**Description:** 
  - This engine manages and executes trading orders using Alpaca's trading API with basic risk management.
**Features:**
  - Connects to Alpaca broker interface.
  - Places buy and sell orders based on signals received.
  - Manages capital allocation and order verification.
  - Integrated dynamic risk management on existing positions
**Usage:** 
- To start the Alpaca Order Management System, run the `AlpacaOMS.py` script. Ensure that your Alpaca API keys are configured properly in the environment variables. Run in PyCharm with emulated terminal environment for best experince.

## Getting Started

### Prerequisites
- Python 3.x
- MongoDB atlas cluster instance ( Free 500mb Cluster available)
- Alpaca account and API keys (Free for API paper Trades)
- Required Python packages (see `requirements.txt`)

### Installation
1. **Clone the repository:**
   git clone https://github.com/nashraj/ThreeEngines.git
   cd ThreeEngines

2. **Install dependencies:**
   pip install -r requirements.txt

3. **Set up environment variables:**
   Create a .env file as "mongodbconfig.env" in the root directory with the following variables:
   MONGODB_USERNAME=Your_MongoDB_Username
   MONGODB_PASSWORD=Your_MongoDB_Password
   MONGODB_CLUSTER=Your_MongoDB_Cluster

   Create another .env file as "alpacaconfig.env" in the root directory with the following header and variables:
   [LOGIN]   
   API_KEY=your_alpaca_api_key
   SECRET_KEY=your_alpaca_secret_key

### Running the Engines
1. **Yahoo Finance MongoDB Interface:**
    python YFinance MongoDB 204 MVP.py

2. **Data Processor:**
   python data processor 204 MVP.py

3. **Alpaca Order Management System:**
   Alpaca OMS MK5 059 MVP.py
   (trading_stream_script.py will automatically initialize in a new window, only for windows systems currently)

### Directory Structure
- ThreeEngines/
- ├── AlpacaOMS.py
- ├── DataProcessor.py
- ├── YFinanceMongoDB.py
- ├── .env
- ├── LICENSE
- ├── README.md
- ├── requirements.txt

### License
This project is licensed under the GNU Affero General Public License v3.0 - see the LICENSE file for details.

### Contributing
Contributions are welcomed! Please read the CONTRIBUTING.md for details on the code of conduct and the process for submitting pull requests.

### Acknowledgements
- Yahoo Finance for market data
- Alpaca for the trading API
- MongoDB for database management

- Contact
For any inquiries or issues, please contact nash@techsixglobal.com.
