# API-Manager-Exchanges (Crypto Funding Rate Arbitrage)

This repository contains Python scripts for implementing funding rate arbitrage strategies on cryptocurrency exchanges. It includes two main strategies:
1. **Futures + Futures Arbitrage** (`root.py`): Arbitrage between perpetual futures contracts across different exchanges.
2. **Spot + Futures Arbitrage** (`root_futures_spot.py`): Arbitrage between spot markets and perpetual futures contracts.

The project leverages asynchronous programming for efficient API calls, pandas for data processing, and the httpx for interacting with cryptocurrency exchange APIs.

## Table of Contents
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Output Table](#output-table)
- [Project Structure](#project-structure)
- [Examples](#examples)
- [Contributing](#contributing)
- [License](#license)

## Features
- Asynchronous API requests to fetch funding rates and market data from multiple exchanges.
- Data processing with pandas for identifying arbitrage opportunities.
- Support for futures-futures and spot-futures arbitrage strategies.
- Modular design for easy extension to additional exchanges or strategies.

## Prerequisites
- **Python**: Version 3.8 or higher.
- **Supported Exchanges**: Bybit, Mexc, Kucoin, Bingx.
- **Dependencies**:
  - `httpx` for exchange API interactions.
  - `pandas` for data manipulation.
  - `asyncio` for asynchronous programming.
  - `numpy` for precise calculations
- A stable internet connection for API requests.

## Installation
1. **Clone the repository**:
   ```bash
   git clone https://github.com/0xRaiseX/API-Manager-Exchanges.git
   cd crypto-funding-arbitrage
   ```

2. **Set up a virtual environment** (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install httpx pandas numpy
   ```

## Usage
The repository contains two main scripts:
- `root.py`: Executes the futures-futures arbitrage strategy.
- `root_futures_spot.py`: Executes the spot-futures arbitrage strategy.

To run a script, use the following command:
```bash
python root.py
```
or
```bash
python root_futures_spot.py
```

## Output Table
The scripts generate a pandas DataFrame that summarizes arbitrage opportunities. Below is an example of the output table produced by the scripts, showing potential arbitrage opportunities between futures and spot markets or between futures on different exchanges.

| Index | exchange_f | symbol_f     | price_f  | funding_rate % | exchange_s | symbol_s     | price_s  | percentage_difference % | %        |
|-------|------------|--------------|----------|----------------|------------|--------------|----------|-------------------------|----------|
| 599   | Bingx      | ASRR/USDT    | 0.306200 | 0.149200       | Mexc       | ASRR/USDT    | 0.298400 | 2.547355                | 2.696555 |
| 191   | Mexc       | FB/USDT      | 0.509900 | 0.026700       | Kucoin     | FB/USDT      | 0.505000 | 0.960973                | 0.987673 |
| 614   | Mexc       | COLLAT/USDT  | 0.047100 | 0.020000       | Bingx      | COLLAT/USDT  | 0.046730 | 0.785563                | 0.805563 |

### Column Descriptions
- **Index**: Unique identifier for the arbitrage opportunity.
- **exchange_f**: Futures exchange (e.g., Bingx, Bybit, Mexc).
- **symbol_f**: Trading pair for the futures contract (e.g., ASRR/USDT).
- **price_f**: Current price of the futures contract.
- **funding_rate %**: Funding rate for the futures contract (positive or negative, in percentage).
- **exchange_s**: Spot exchange (or second futures exchange for futures-futures arbitrage).
- **symbol_s**: Trading pair for the spot market (or second futures contract).
- **price_s**: Current price of the spot market (or second futures contract).
- **percentage_difference %**: Price difference between futures and spot (or between two futures), calculated as `((price_f - price_s) / price_s) * 100`.
- **%**: Adjusted percentage difference, potentially including fees or other costs.

This table helps identify pairs with significant price differences and favorable funding rates for arbitrage.

## Project Structure
```plaintext
crypto-funding-arbitrage/
│
├── root.py                 # Futures + Futures arbitrage script
├── root_futures_spot.py    # Spot + Futures arbitrage script
├── LICENSE                 # Lisense for project
└── README.md               # This file
```

- `root.py`: Implements arbitrage between perpetual futures contracts across exchanges.
- `root_futures_spot.py`: Implements arbitrage between spot markets and futures contracts.


## Contributing
Contributions are welcome! To contribute:
1. Fork the repository.
2. Create a new branch (`git checkout -b feature/your-feature`).
3. Make your changes and commit (`git commit -m "Add your feature"`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Open a Pull Request.

Please ensure your code follows PEP 8 guidelines and includes appropriate documentation.

## License
This project is licensed under the GNU General Public License v3.0. See the LICENSE file for details. The code is provided for demonstration purposes, and any use, modification, or distribution must comply with the GPL-3.0 terms. Commercial use requires explicit permission from the repository owner.