from datetime import datetime, timedelta
import httpx
import asyncio
import time
import pandas as pd
import numpy as np
from collections import defaultdict

"""
ФЬЮЧЕРС + СПОТ
Когда цена фьючерса выше, чем спота. 

        ФЬЮЧЕРС
--------------------------- 
        СПОТ                
___________________________

"""

class Bybit:
    def __init__(self) -> None:
        self.primary_data = {} # {биржевый символ:стандартный символ}
        self.primary_data_spot = {} # {биржевый символ:стандартный символ} spot
        self.reverse_data = {} # {стандартный символ:биржевый символ}
        self.reverse_data_spot = {} # {стандартный символ:биржевый символ} spot
        self.funding_rates = {} # {стандартный символ:текущая ставка финансиварония} Ранее self.symbols
        self.symbols_prices = {} # {стандартный символ:{ask1, bid1}}
        self.symbols_prices_spot = {} # {стандартный символ:{ask1, bid1}} spot
        self.rate_times = {} # {стандартный символ:время выплаты финансирования} Время выплаты в Москвской формате (+3 часа)

        self.TAKER_FEE = 0.0011
        self.MAKER_FEE = 0.00036

    """Загрузка первичных, необходимых данных для работы с символами биржи"""
    async def _get_primary_data(self):
        """Можно получить даннеы для каждого символа: плечо, lotsize"""
        async with httpx.AsyncClient() as client:
            errors = 0
            response = await client.get("https://api.bybit.com/v5/market/instruments-info?category=linear") # Подгружаем символы для фьючерсов
            for symbol in response.json()['result']['list']:
                try:
                    exchange_symbol = symbol['symbol']  # Символ биржи (например, 'BTCUSDT')
                    standard_symbol = f"{symbol['baseCoin']}/{symbol['quoteCoin']}"  # В стандартном виде 'BTC/USDT'

                    self.primary_data[exchange_symbol] = standard_symbol
                    self.reverse_data[standard_symbol] = exchange_symbol
                except ValueError:
                    errors += 1
                    continue

        async with httpx.AsyncClient() as client:
            errors = 0
            response = await client.get("https://api.bybit.com/v5/market/instruments-info?category=spot") # Подгружаем символы для спота
            for symbol in response.json()['result']['list']:
                try:
                    exchange_symbol = symbol['symbol']  # Символ биржи (например, 'BTCUSDT')
                    standard_symbol = f"{symbol['baseCoin']}/{symbol['quoteCoin']}"  # В стандартном виде 'BTC/USDT'

                    self.primary_data_spot[exchange_symbol] = standard_symbol
                    self.reverse_data_spot[standard_symbol] = exchange_symbol
                except ValueError:
                    errors += 1
                    continue


    """Основная функция, которая загружает все первичные + основные данные по символам для биржи"""
    async def main__get_symbols(self):
        if len(self.primary_data) == 0:
            await self._get_primary_data() # Символы с биржи загружены

        async with httpx.AsyncClient() as client:
            response = await client.get("https://api.bybit.com/v5/market/tickers?category=linear")
            for symbol in response.json()['result']['list']:
                try:
                    key = self.primary_data[symbol['symbol']]
                    self.funding_rates[key] = float(symbol['fundingRate'])
                    self.symbols_prices[key] = {'ask': float(symbol['ask1Price']), 'bid': float(symbol['bid1Price'])}
                    self.rate_times[key] = int(symbol['nextFundingTime']) + 3 * 60 * 60 * 1000
                except Exception:
                    continue

        async with httpx.AsyncClient() as client:
            response = await client.get("https://api.bybit.com/v5/market/tickers?category=spot")
            for symbol in response.json()['result']['list']:
                try:
                    key = self.primary_data_spot[symbol['symbol']]
                    self.symbols_prices_spot[key] = {'ask': float(symbol['ask1Price']), 'bid': float(symbol['bid1Price'])}
                except Exception:
                    continue

    def get_name(self):
        return "Bybit"
    
    def to_standard(self, exchange_symbol: str) -> str:
        """Переводит биржевой символ в стандартный"""
        return self.primary_data.get(exchange_symbol, exchange_symbol)

    def to_exchange(self, standard_symbol: str) -> str:
        """Переводит стандартный символ в формат биржи"""
        return self.reverse_data.get(standard_symbol, standard_symbol)

    """Удаляет символы из self.funding_rates, у который ставка финансрования меньше 0.01%"""
    def reset_not_valid_pair(self):
        new_data = {}
        for symbol, funding_rate in self.funding_rates.items():
            if funding_rate > 0.0001:
                new_data[symbol] = funding_rate
        self.funding_rates = new_data


class Mexc:
    def __init__(self) -> None:
        self.primary_data = {} # {биржевый символ:стандартный символ}
        self.primary_data_spot = {} # {биржевый символ:стандартный символ} spot
        self.reverse_data = {} # {стандартный символ:биржевый символ}
        self.reverse_data_spot = {} # {стандартный символ:биржевый символ} spot
        self.funding_rates = {} # {стандартный символ:текущая ставка финансиварония} Ранее self.symbols
        self.symbols_prices = {} # {стандартный символ:{ask1, bid1}}
        self.symbols_prices_spot = {} # {стандартный символ:{ask1, bid1}} spot
        self.rate_times = {} # {стандартный символ:время выплаты финансирования} Время выплаты в Москвской формате (+3 часа)

        self.TAKER_FEE = 0.0002
        self.MAKER_FEE = 0

    """Загрузка первичных, необходимых данных для работы с символами биржи"""
    async def _get_primary_data(self):
        async with httpx.AsyncClient() as client:
            errors = 0
            response = await client.get("https://contract.mexc.com/api/v1/contract/detail")
            for symbol in response.json()['data']:
                try:
                    exchange_symbol = symbol['symbol']  # Символ биржи (например, 'BTCUSDT')
                    standard_symbol = f"{symbol['baseCoin']}/{symbol['quoteCoin']}"  # В стандартном виде 'BTC/USDT'

                    self.primary_data[exchange_symbol] = standard_symbol
                    self.reverse_data[standard_symbol] = exchange_symbol
                except ValueError:
                    errors += 1
                    continue

        async with httpx.AsyncClient() as client:
            errors = 0
            response = await client.get("https://api.mexc.com/api/v3/exchangeInfo")

            for symbol in response.json()['symbols']:
                try:
                    exchange_symbol = symbol['symbol']  # Символ биржи (например, 'BTCUSDT')
                    standard_symbol = f"{symbol['baseAsset']}/{symbol['quoteAsset']}"  # В стандартном виде 'BTC/USDT'
                    if symbol['isSpotTradingAllowed']:
                        self.primary_data_spot[exchange_symbol] = standard_symbol
                        self.reverse_data_spot[standard_symbol] = exchange_symbol
                except ValueError:
                    errors += 1
                    continue

    """Запрос ставки финансирования на отдельный символ"""
    async def fetch_funding_rate(self, client, symbol):
        try:
            symbol_mexc = symbol.split('/')[0] + "_" + symbol.split('/')[1]
            try:
                response = await client.get(f"https://contract.mexc.com/api/v1/contract/funding_rate/{symbol_mexc}")
            except Exception as e:
                print(e)
                return
            data = response.json()
            self.rate_times[symbol] = float(data['data']['nextSettleTime']) + 3 * 60 * 60 * 1000
        except KeyError:
            pass

    """Генерация запросов для подходящих символов"""
    async def fetch_all_funding_rates(self, symbols):
        async with httpx.AsyncClient() as client:
            tasks = [self.fetch_funding_rate(client, symbol) for symbol in symbols]
            await asyncio.gather(*tasks)

    """Основная функция, которая загружает все первичные + основные данные по символам для биржи"""
    async def main__get_symbols(self):
        if len(self.primary_data) == 0:
            await self._get_primary_data() # Символы с биржи загружены

        async with httpx.AsyncClient() as client:
            response = await client.get("https://contract.mexc.com/api/v1/contract/ticker")
            symbols_local = []
            for symbol in response.json()['data']:
                try:
                    key = self.primary_data[symbol['symbol']]
                    self.funding_rates[key] = float(symbol['fundingRate'])
                    self.symbols_prices[key] = {'ask': float(symbol['ask1']), 'bid': float(symbol['bid1'])}
                except Exception:
                    continue

            for key, value in self.funding_rates.items():
                if abs(value) > 0.0005:
                    symbols_local.append(key)
        
            await self.fetch_all_funding_rates(symbols_local)

        target_hours = [3, 11, 19]
        def nearest_time_to_targets(current_time):
            current_hour_minute = current_time.hour + current_time.minute / 60.0
            future_hours = [hour for hour in target_hours if hour > current_hour_minute]
            if not future_hours:
                nearest_hour = min(target_hours)
                target_datetime = current_time.replace(hour=nearest_hour, minute=0, second=0, microsecond=0) + timedelta(days=1)
            else:
                nearest_hour = min(future_hours)
                target_datetime = current_time.replace(hour=nearest_hour, minute=0, second=0, microsecond=0)
            return int(target_datetime.timestamp() * 1000)
        
        current_time = datetime.now()
        nearest_unix_time_ms = nearest_time_to_targets(current_time)

        for symbol in self.funding_rates:
            if symbol not in self.rate_times.keys():
                self.rate_times[symbol] = nearest_unix_time_ms

        async with httpx.AsyncClient() as client:
            response = await client.get("https://api.mexc.com/api/v3/ticker/bookTicker")
            for symbol in response.json():
                try:
                    key = self.primary_data_spot[symbol['symbol']]
                    self.symbols_prices_spot[key] = {'ask': float(symbol['askPrice']), 'bid': float(symbol['bidPrice'])}
                except Exception:
                    continue

    def get_name(self):
        return "Mexc"
    
    def to_standard(self, exchange_symbol: str) -> str:
        """Переводит биржевой символ в стандартный"""
        return self.primary_data.get(exchange_symbol, exchange_symbol)

    def to_exchange(self, standard_symbol: str) -> str:
        """Переводит стандартный символ в формат биржи"""
        return self.reverse_data.get(standard_symbol, standard_symbol)

    """Удаляет символы из self.funding_rates, у который ставка финансрования меньше 0.01%"""
    def reset_not_valid_pair(self):
        new_data = {}
        for symbol, funding_rate in self.funding_rates.items():
            if funding_rate > 0.0001:
                new_data[symbol] = funding_rate
        self.funding_rates = new_data


class Bingx:
    def __init__(self) -> None:
        self.primary_data = {} # {биржевый символ:стандартный символ}
        self.primary_data_spot = {} # {биржевый символ:стандартный символ} spot
        self.reverse_data = {} # {стандартный символ:биржевый символ}
        self.reverse_data_spot = {} # {стандартный символ:биржевый символ} spot
        self.funding_rates = {} # {стандартный символ:текущая ставка финансиварония} Ранее self.symbols
        self.symbols_prices = {} # {стандартный символ:{ask1, bid1}}
        self.symbols_prices_spot = {} # {стандартный символ:{ask1, bid1}} spot
        self.rate_times = {} # {стандартный символ:время выплаты финансирования} Время выплаты в Москвской формате (+3 часа)

        self.TAKER_FEE = 0.0005
        self.MAKER_FEE = 0.0002

    """Загрузка первичных, необходимых данных для работы с символами биржи"""
    async def _get_primary_data(self):
        """Можно получить даннеы для каждого символа: плечо, lotsize"""
        async with httpx.AsyncClient() as client:
            errors = 0
            response = await client.get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts")
            for symbol in response.json()['data']:
                try:
                    exchange_symbol = symbol['symbol']  # Символ биржи (например, 'BTCUSDT')
                    standard_symbol = f"{symbol['symbol'].split('-')[0]}/{symbol['symbol'].split('-')[1]}"  # В стандартном виде 'BTC/USDT'

                    self.primary_data[exchange_symbol] = standard_symbol
                    self.reverse_data[standard_symbol] = exchange_symbol
                except ValueError:
                    errors += 1
                    continue

        async with httpx.AsyncClient() as client:
            errors = 0
            response = await client.get("https://open-api.bingx.com/openApi/spot/v1/common/symbols") # Подгружаем символы для спота
            for symbol in response.json()['data']['symbols']:
                try:
                    exchange_symbol = symbol['symbol']  # Символ биржи (например, 'BTCUSDT')
                    standard_symbol = f"{symbol['symbol'].split('-')[0]}/{symbol['symbol'].split('-')[1]}"  # В стандартном виде 'BTC/USDT'

                    self.primary_data_spot[exchange_symbol] = standard_symbol
                    self.reverse_data_spot[standard_symbol] = exchange_symbol
                except ValueError:
                    errors += 1
                    continue

    async def _load_additional_data(self):
        async with httpx.AsyncClient() as client:
            response = await client.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker")
            for symbol in response.json()['data']:
                try:
                    key = self.primary_data[symbol['symbol']]
                    self.symbols_prices[key] = {'ask': float(symbol['askPrice']), 'bid': float(symbol['bidPrice'])}
                except Exception:
                    continue

    """Основная функция, которая загружает все первичные + основные данные по символам для биржи"""
    async def main__get_symbols(self):
        if len(self.primary_data) == 0:
            await self._get_primary_data() # Символы с биржи загружены

        await self._load_additional_data()
        
        async with httpx.AsyncClient() as client:
            response = await client.get("https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex")
            for symbol in response.json()['data']:
                try:
                    key = self.primary_data[symbol['symbol']]
                    if key in self.symbols_prices:
                        self.funding_rates[key] = float(symbol['lastFundingRate'])
                        self.rate_times[key] = int(symbol['nextFundingTime']) + 3 * 60 * 60 * 1000
                except Exception:
                    continue

        async with httpx.AsyncClient() as client:
            response = await client.get("https://open-api.bingx.com/openApi/spot/v1/ticker/24hr"+"?timestamp="+str(int(time.time() * 1000)))
            for symbol in response.json()['data']:
                try:
                    key = self.primary_data_spot[symbol['symbol']]
                    self.symbols_prices_spot[key] = {'ask': float(symbol['askPrice']), 'bid': float(symbol['bidPrice'])}
                except Exception:
                    continue

    def get_name(self):
        return "Bingx"
    
    def to_standard(self, exchange_symbol: str) -> str:
        """Переводит биржевой символ в стандартный"""
        return self.primary_data.get(exchange_symbol, exchange_symbol)

    def to_exchange(self, standard_symbol: str) -> str:
        """Переводит стандартный символ в формат биржи"""
        return self.reverse_data.get(standard_symbol, standard_symbol)

    """Удаляет символы из self.funding_rates, у который ставка финансрования меньше 0.01%"""
    def reset_not_valid_pair(self):
        new_data = {}
        for symbol, funding_rate in self.funding_rates.items():
            if funding_rate > 0.0001:
                new_data[symbol] = funding_rate
        self.funding_rates = new_data

class Kucoin:
    def __init__(self) -> None:
        self.primary_data = {} # {биржевый символ:стандартный символ}
        self.primary_data_spot = {} # {биржевый символ:стандартный символ} spot
        self.reverse_data = {} # {стандартный символ:биржевый символ}
        self.reverse_data_spot = {} # {стандартный символ:биржевый символ} spot
        self.funding_rates = {} # {стандартный символ:текущая ставка финансиварония} Ранее self.symbols
        self.symbols_prices = {} # {стандартный символ:{ask1, bid1}}
        self.symbols_prices_spot = {} # {стандартный символ:{ask1, bid1}} spot
        self.rate_times = {} # {стандартный символ:время выплаты финансирования} Время выплаты в Москвской формате (+3 часа)

        self.TAKER_FEE = 0.0006
        self.MAKER_FEE = 0.0002

    """Загрузка первичных, необходимых данных для работы с символами биржи"""
    async def _get_primary_data(self):
        """Можно получить даннеы для каждого символа: плечо, lotsize"""
        async with httpx.AsyncClient() as client:
            errors = 0
            response = await client.get("https://api-futures.kucoin.com/api/v1/contracts/active")
            for symbol in response.json()['data']:
                try:
                    exchange_symbol = symbol['symbol']  # Символ биржи (например, 'BTCUSDT')
                    standard_symbol = f"{symbol['baseCurrency']}/{symbol['quoteCurrency']}"  # В стандартном виде 'BTC/USDT'

                    self.primary_data[exchange_symbol] = standard_symbol
                    self.reverse_data[standard_symbol] = exchange_symbol
                except TypeError:
                    errors += 1
                    continue

        async with httpx.AsyncClient() as client:
            errors = 0
            response = await client.get("https://api.kucoin.com/api/v2/symbols") # Подгружаем символы для спота
            for symbol in response.json()['data']:
                try:
                    exchange_symbol = symbol['symbol']  # Символ биржи (например, 'BTCUSDT')
                    standard_symbol = f"{symbol['baseCurrency']}/{symbol['quoteCurrency']}"  # В стандартном виде 'BTC/USDT'
                    if symbol['enableTrading']:
                        self.primary_data_spot[exchange_symbol] = standard_symbol
                        self.reverse_data_spot[standard_symbol] = exchange_symbol
                except ValueError:
                    errors += 1
                    continue

    async def _get_symbols_prices(self):
        async with httpx.AsyncClient() as client:
            response = await client.get("https://api-futures.kucoin.com/api/v1/allTickers")
            for symbol in response.json()['data']:
                try:
                    key = self.primary_data[symbol['symbol']]
                    self.symbols_prices[key] = {'ask': float(symbol['bestAskPrice']), 'bid': float(symbol['bestBidPrice'])}
                except Exception:
                    continue

    """Основная функция, которая загружает все первичные + основные данные по символам для биржи"""
    async def main__get_symbols(self):
        if len(self.primary_data) == 0:
            await self._get_primary_data() # Символы с биржи загружены

        await self._get_symbols_prices()

        async with httpx.AsyncClient() as client:
            errors = 0
            response = await client.get("https://api-futures.kucoin.com/api/v1/contracts/active")
            for symbol in response.json()['data']:
                try:
                    key = self.primary_data[symbol['symbol']]
                    self.funding_rates[key] = float(symbol['fundingFeeRate'])
                    current_time = float(symbol['nextFundingRateTime']) + time.time() * 1000 + 3 * 60 * 60 * 1000
                    rounded_time = int(current_time // 10000) * 10000
                    self.rate_times[key] = rounded_time
                except Exception:
                    errors += 1
                    continue

        async with httpx.AsyncClient() as client:
            errors = 0
            response = await client.get("https://api.kucoin.com/api/v1/market/allTickers")
            for symbol in response.json()['data']['ticker']:
                try:
                    key = self.primary_data_spot[symbol['symbol']]
                    self.symbols_prices_spot[key] = {'ask': float(symbol['buy']), 'bid': float(symbol['sell'])}
                except Exception:
                    errors += 1
                    continue

    def get_name(self):
        return "Kucoin"
    
    def to_standard(self, exchange_symbol: str) -> str:
        """Переводит биржевой символ в стандартный"""
        return self.primary_data.get(exchange_symbol, exchange_symbol)

    def to_exchange(self, standard_symbol: str) -> str:
        """Переводит стандартный символ в формат биржи"""
        return self.reverse_data.get(standard_symbol, standard_symbol)

    """Удаляет символы из self.funding_rates, у который ставка финансрования меньше 0.01%"""
    def reset_not_valid_pair(self):
        new_data = {}
        for symbol, funding_rate in self.funding_rates.items():
            if funding_rate > 0.0001:
                new_data[symbol] = funding_rate
        self.funding_rates = new_data

async def main():
    objects = [Bybit(), Kucoin(), Mexc(), Bingx()]
    load_objects = []
    for i in objects:
        load_objects.append(i.main__get_symbols())
    await asyncio.gather(*load_objects)

    for exchange in objects:
        exchange.reset_not_valid_pair()

    symbol_to_exchanges_futures = defaultdict(list)
    symbol_to_exchanges_spot = defaultdict(list)

    stop = {
        "Bybit": ['MexcQI/USDT', 'BybitFB/USDT'],
        "Mexc": ['MexcQI/USDT', 'BybitFB/USDT']
    }

    errors = 0
    for exchange_futures in objects:
        for symbol_futures, bid_ask_futures in exchange_futures.symbols_prices.items():
            try:
                symbol_to_exchanges_futures[symbol_futures].append((exchange_futures.get_name(), symbol_futures, bid_ask_futures, exchange_futures.funding_rates[symbol_futures]))
            except KeyError:
                errors += 1

    for exchange_spot in objects:
        for symbol_spot, bid_ask_spot in exchange_spot.symbols_prices_spot.items():
            symbol_to_exchanges_spot[symbol_spot].append((exchange_spot.get_name(), symbol_spot, bid_ask_spot))

    data = []
    for symbol in symbol_to_exchanges_futures:
        if symbol in symbol_to_exchanges_spot:
            for exchange_futures_name, symbol_futures, bid_ask_futures, funding_rate in symbol_to_exchanges_futures[symbol]:
                for exchange_spot_name, symbol_spot, bid_ask_spot in symbol_to_exchanges_spot[symbol]:
                    try:
                        if exchange_spot_name+symbol_spot in stop[exchange_futures_name]:
                            continue
                    except KeyError:
                        pass
                    data.append([
                        exchange_futures_name,
                        symbol_futures,
                        bid_ask_futures['bid'],
                        funding_rate * 100,
                        exchange_spot_name,
                        symbol_spot,
                        bid_ask_spot['ask']
                    ])
                
    dataFrame = pd.DataFrame(data, columns=['exchange_f', 'symbol_f', 'price_f', 'funding_rate %', 'exchange_s', 'symbol_s', 'price_s'])  
    dataFrame['percentage_difference %'] = 100 - (dataFrame['price_s'] / dataFrame['price_f']) * 100
    dataFrame.sort_values(by="funding_rate %", inplace=True, ascending=False)
    
    dataFrame = dataFrame[dataFrame['percentage_difference %'].abs() <= 60]
    dataFrame = dataFrame[dataFrame['percentage_difference %'] >= 0.5]

    dataFrame['%'] = dataFrame['funding_rate %'] + dataFrame['percentage_difference %']

    print(dataFrame)


asyncio.run(main())