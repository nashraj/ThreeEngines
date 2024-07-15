# Three Engines Project

## Overview
This project consists of three main components that work together to manage and execute trading strategies using market data from Yahoo Finance, process the data, and place orders through the Alpaca broker interface.

## Components

### 1. Alpaca Order Management System (AlpacaOMS.py)
- **Description:** This engine manages and executes trading orders using Alpaca's trading API.
- **Features:**
  - Connects to Alpaca broker interface.
  - Places buy and sell orders based on signals received.
  - Manages capital allocation and order verification.
- **Usage:** To start the Alpaca Order Management System, run the `AlpacaOMS.py` script. Ensure that your Alpaca API keys are configured properly in the environment variables.

### 2. Data Processor (DataProcessor.py)
- **Description:** This engine processes market data to generate trading signals.
- **Features:**
  - Fetches market data from the MongoDB database.
  - Applies proprietary trading strategy algorithms.
  - Sends trading signals to the order management system.
- **Usage:** To start the Data Processor, run the `DataProcessor.py` script. The script will fetch data from the MongoDB database, process it, and send signals based on the defined strategy.

### 3. Yahoo Finance MongoDB Interface (YFinanceMongoDB.py)
- **Description:** This engine handles fetching market data from Yahoo Finance and storing it in a MongoDB database.
- **Features:**
  - Fetches market data using Yahoo Finance API.
  - Stores fetched data in MongoDB.
  - Updates the database with incremental changes.
- **Usage:** To start the Yahoo Finance MongoDB Interface, run the `YFinanceMongoDB.py` script. Make sure your MongoDB connection parameters are set in the environment variables.

## Getting Started

### Prerequisites
- Python 3.x
- MongoDB instance
- Alpaca account and API keys
- Required Python packages (see `requirements.txt`)

### Installation
1. **Clone the repository:**
   git clone https://github.com/nashraj/ThreeEngines.git
   cd ThreeEngines

2. **Install dependencies:**
   pip install -r requirements.txt

3. **Set up environment variables:**
   Create a .env file in the root directory with the following variables:
   MONGODB_URI=mongodb://your_username:your_password@your_cluster.mongodb.net/?retryWrites=true&w=majority
   ALPACA_API_KEY=your_alpaca_api_key
   ALPACA_SECRET_KEY=your_alpaca_secret_key

### Running the Engines
1. **Yahoo Finance MongoDB Interface:**
    python YFinanceMongoDB.py

2. **Data Processor:**
   python DataProcessor.py

3. **Alpaca Order Management System:**
   python AlpacaOMS.py

### Directory Structure
ThreeEngines/
├── AlpacaOMS.py
├── DataProcessor.py
├── YFinanceMongoDB.py
├── .env
├── LICENSE
├── README.md
├── requirements.txt

### License
This project is licensed under the GNU Affero General Public License v3.0 - see the LICENSE file for details.

### Contributing
Contributions are welcomed! Please read the CONTRIBUTING.md for details on the code of conduct and the process for submitting pull requests.

### Acknowledgements
- Yahoo Finance for market data
- Alpaca for the trading API
- MongoDB for database management

- Contact
For any inquiries or issues, please contact your-email@example.com.
