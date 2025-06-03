from datetime import datetime, timedelta
import httpx
import requests
import asyncio
import time
import pandas as pd
import numpy as np

class Bybit:
    def __init__(self) -> None:
        self.symbols = {}
        self.primary_data = {}
        self.symbols_prices = {}
        self.rate_times = {}

        self.TAKER_FEE = 0.11 # % 0.0011
        self.MAKER_FEE = 0.036 # % 0.00036

    async def _get_primary_data(self):
        async with httpx.AsyncClient() as client:
            data = await client.get("https://api.bybit.com/v5/market/instruments-info?category=linear")
            data = data.json()['result']['list']
            for symbol in data:
                try:
                    self.primary_data[symbol['symbol']] = symbol['baseCoin'] + "/" +symbol['quoteCoin']
                except ValueError:
                    continue
        
    async def get_symbols_from_exchange(self):
        if len(self.primary_data) == 0:
            await self._get_primary_data()

        async with httpx.AsyncClient() as client:
            data = await client.get("https://api.bybit.com/v5/market/tickers?category=linear")
            data = data.json()['result']['list']
            for symbol in data:
                try:
                    key = self.primary_data[symbol['symbol']]
                    self.symbols[key] = float(symbol['fundingRate']) * 100

                    self.symbols_prices[key] = {'ask': float(symbol['ask1Price']), 'bid': float(symbol['bid1Price'])}

                    self.rate_times[key] = int(symbol['nextFundingTime']) + 3 * 60 * 60 * 1000
                except Exception:
                    continue

    def get_name(self):
        return "Bybit"

class Mexc:
    def __init__(self) -> None:
        self.symbols = {}
        self.primary_data = {}
        self.symbols_prices = {}
        self.rate_times = {}

        self.TAKER_FEE = 0.02 # % 0.0011
        self.MAKER_FEE = 0 # % 0.00036

    async def _get_primary_data(self):
        async with httpx.AsyncClient() as client:
            data = await client.get("https://contract.mexc.com/api/v1/contract/detail")
            data = data.json()
            for symbol in data['data']:
                try:
              
                    self.primary_data[symbol['symbol']] = symbol['baseCoin'] + "/" +symbol['quoteCoin']
                except ValueError:
                    continue
    
    async def fetch_funding_rate(self, client, symbol):
        try:
            symbol_mexc = symbol.split('/')[0] + "_" + symbol.split('/')[1]
            response = await client.get(f"https://contract.mexc.com/api/v1/contract/funding_rate/{symbol_mexc}")
            data = response.json()
            self.rate_times[symbol] = float(data['data']['nextSettleTime']) + 3 * 60 * 60 * 1000
        except KeyError:
            pass

    async def fetch_all_funding_rates(self, symbols):
        async with httpx.AsyncClient() as client:
            tasks = [self.fetch_funding_rate(client, symbol) for symbol in symbols]
            
            await asyncio.gather(*tasks)
        
    async def get_symbols_from_exchange(self):
        if len(self.primary_data) == 0:
            await self._get_primary_data()

        async with httpx.AsyncClient() as client:
            data = await client.get("https://contract.mexc.com/api/v1/contract/ticker")
            data = data.json()

            symbols_local = []

            for symbol in data['data']:
                try:
                    key = self.primary_data[symbol['symbol']]
                    self.symbols[key] = float(symbol['fundingRate']) * 100

                    self.symbols_prices[key] = {'ask': float(symbol['ask1']), 'bid': float(symbol['bid1'])}
                except Exception:
                    continue

            for key, value in self.symbols.items():
                if abs(value) > 0.05:
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

        for symbol in self.symbols:
            if symbol not in self.rate_times.keys():
                self.rate_times[symbol] = nearest_unix_time_ms

    def get_name(self):
        return "Mexc"

class Bingx:
    def __init__(self) -> None:
        self.symbols = {}
        self.primary_data = {}
        self.symbols_prices = {}
        self.rate_times = {}

        self.TAKER_FEE = 0.05 # % 0.0005
        self.MAKER_FEE = 0.02 # % 0.0002

    async def _get_primary_data(self):
        async with httpx.AsyncClient() as client:
            data = await client.get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts")
            data = data.json()
            for symbol in data['data']:
                try:
                    self.primary_data[symbol['symbol']] = symbol['symbol'].split('-')[0] + "/" + symbol['symbol'].split('-')[1]
                except ValueError:
                    continue
    
    async def _load_additional_data(self):
        async with httpx.AsyncClient() as client:
            data = await client.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker")
            data = data.json()
            for symbol in data['data']:
                try:
                    key = self.primary_data[symbol['symbol']]
                    self.symbols_prices[key] = {'ask': float(symbol['askPrice']), 'bid': float(symbol['bidPrice'])}
                except Exception:
                    continue

    async def get_symbols_from_exchange(self):
        if len(self.primary_data) == 0:
            await self._get_primary_data()

        await self._load_additional_data()

        async with httpx.AsyncClient() as client:
            data = await client.get("https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex")
            data = data.json()
            for symbol in data['data']:
                try:
                    key = self.primary_data[symbol['symbol']]
                    if key in self.symbols_prices:
                        self.symbols[key] = float(symbol['lastFundingRate']) * 100
                    self.rate_times[key] = int(symbol['nextFundingTime']) + 3 * 60 * 60 * 1000
                except Exception:
                    continue

    def get_name(self):
        return "Bingx"

class Kucoin:
    def __init__(self) -> None:
        self.symbols = {}
        self.primary_data = {}
        self.symbols_prices = {}
        self.rate_times = {}

        self.TAKER_FEE = 0.06 # % 0.0006
        self.MAKER_FEE = 0.02 # % 0.0002
        
    async def _get_primary_data(self):
        async with httpx.AsyncClient() as client:
            data = await client.get("https://api-futures.kucoin.com/api/v1/contracts/active")
            data = data.json()
            for symbol in data['data']:
                try:
                    self.primary_data[symbol['symbol']] = symbol['baseCurrency'] + "/" + symbol['quoteCurrency']
                except TypeError:
                    continue
    
    async def _get_symbols_prices(self):
        async with httpx.AsyncClient() as client:
                data = await client.get("https://api-futures.kucoin.com/api/v1/allTickers")
                data = data.json()
                for symbol in data['data']:
                    try:
                        key = self.primary_data[symbol['symbol']]
                        self.symbols_prices[key] = {'ask': float(symbol['bestAskPrice']), 'bid': float(symbol['bestBidPrice'])}
                    except Exception:
                        continue

    async def get_symbols_from_exchange(self):
        if len(self.primary_data) == 0:
            await self._get_primary_data()

        await self._get_symbols_prices()

        async with httpx.AsyncClient() as client:
            data = await client.get("https://api-futures.kucoin.com/api/v1/contracts/active")
            data = data.json()
            for symbol in data['data']:
                try:
                    key = self.primary_data[symbol['symbol']]
                    self.symbols[key] = float(symbol['fundingFeeRate']) * 100
                    current_time = float(symbol['nextFundingRateTime']) + time.time() * 1000 + 3 * 60 * 60 * 1000

                    rounded_time = int(current_time // 10000) * 10000
                    self.rate_times[key] = rounded_time
                except Exception:
                    continue
    
    def get_name(self):
        return "Kucoin"
    
async def main():
    bybit = Bybit()
    kucoin = Kucoin()
    mexc = Mexc()
    bingx = Bingx()

    objects = [bybit, kucoin, mexc, bingx]

    load_objects = []
    for i in objects:
        load_objects.append(i.get_symbols_from_exchange())
    
    await asyncio.gather(*load_objects)

    main_dict = []

    for exchange in objects:
        sorted_dict = dict(sorted(exchange.symbols.items(), key=lambda item: abs(item[1]), reverse=True))
        filtered_dict = {key: value for key, value in sorted_dict.items() if abs(value) >= 0.1}

        for key, value in filtered_dict.items():
            for haghe_exchange in objects:
                if key in haghe_exchange.symbols:
                    if exchange != haghe_exchange:
                        if value < 0:
                            price_main_symbol = exchange.symbols_prices[key]['ask']
                            price_hedge_symbol = haghe_exchange.symbols_prices[key]['bid']
                            
                            final_difference = 100 - price_main_symbol / price_hedge_symbol * 100
                            main_route = "LONG"
                            hedge_route = "SHORT"
                        else:
                            price_main_symbol = exchange.symbols_prices[key]['bid']
                            price_hedge_symbol = haghe_exchange.symbols_prices[key]['ask']

                            final_difference = 100 - price_hedge_symbol / price_main_symbol * 100
                            main_route = "SHORT"
                            hedge_route = "LONG"
                        try:
                            time_1 = exchange.rate_times[key]
                        except KeyError:
                            time_1 = None
                        try:
                            time_2 = haghe_exchange.rate_times[key]
                        except KeyError:
                            time_2 = None


                        fee = exchange.TAKER_FEE * 2 + haghe_exchange.TAKER_FEE * 2
                        main_dict.append([key, exchange.get_name(), main_route, haghe_exchange.get_name(), hedge_route, value, haghe_exchange.symbols[key], price_main_symbol, price_hedge_symbol, final_difference, fee, time_1, time_2])   

    dataFrame = pd.DataFrame(main_dict, columns=['symbol', 'main_exchange', 'route_1', 'hadge_exchange', 'route_2', 'rate_1(%)', 'rate_2(%)', 'price_1', 'price_2', 'price_difference(%)', 'fee(%)', 'time_1', 'time_2'])  
    dataFrame.sort_values(by="rate_1(%)", inplace=True, key=abs, ascending=False)

    dataFrame = dataFrame.loc[(dataFrame['time_1'] <= dataFrame['time_2']) | (dataFrame['time_1'].isna()) | (dataFrame['time_2'].isna())]
    dataFrame['result(%)'] = np.where(
        (dataFrame['time_1'] == dataFrame['time_2']) | 
        (pd.isna(dataFrame['time_1']) | pd.isna(dataFrame['time_2'])), 
        np.where(
            (dataFrame['rate_1(%)'] > 0) & (dataFrame['rate_2(%)'] > 0),
            dataFrame['rate_1(%)'] - dataFrame['rate_2(%)'],
            np.where(
                (dataFrame['rate_1(%)'] < 0) & (dataFrame['rate_2(%)'] < 0),
                abs(dataFrame['rate_1(%)']) - abs(dataFrame['rate_2(%)']),
                np.where(
                    (dataFrame['rate_1(%)'] > 0) & (dataFrame['rate_2(%)'] < 0),
                    abs(dataFrame['rate_1(%)']) + abs(dataFrame['rate_2(%)']),
                    np.where(
                        (dataFrame['rate_1(%)'] < 0) & (dataFrame['rate_2(%)'] > 0),
                        abs(dataFrame['rate_1(%)']) + abs(dataFrame['rate_2(%)']),
                        abs(dataFrame['rate_1(%)'])
                    )
                )
            )
        ),
        abs(dataFrame['rate_1(%)']) 
    )

    dataFrame['result(%)'] = dataFrame['result(%)'] + dataFrame['price_difference(%)']
    dataFrame['result(%)'] = dataFrame['result(%)'] - dataFrame['fee(%)']
    dataFrame.sort_values(by="result(%)", inplace=True, ascending=False)

    dataFrame2 = dataFrame.copy()
    dataFrame2['time_1'] = pd.to_datetime(dataFrame2['time_1'], unit='ms')
    dataFrame2['time_1'] = dataFrame2['time_1'].dt.strftime('%m-%d %H:%M:%S')
    dataFrame2['time_2'] = pd.to_datetime(dataFrame2['time_2'], unit='ms')
    dataFrame2['time_2'] = dataFrame2['time_2'].dt.strftime('%m-%d %H:%M:%S')
    print(dataFrame2)

    rows_with_max_value = dataFrame[dataFrame["result(%)"] > 0].copy()
    rows_with_max_value['time_1'] = pd.to_datetime(rows_with_max_value['time_1'], unit='ms')
    rows_with_max_value['time_2'] = pd.to_datetime(rows_with_max_value['time_2'], unit='ms')

asyncio.run(main())

"""
symbol - текущий символ, для которого высчитывается Funding Rate
main_exchange - основная биржа, относительно которой выполняются вычисления
route_1 - направление сделки на основной бирже
hadge_exchange - биржа для хержирования позиции
route_2 - направление сделки на бирже хеджирования
rate_1(%) - ставка финансирования на основной бирже (в процентах)
rate_2(%) - ставка финансирования на бирже хеджирования (в процентах)
price_1 - текущая стоимость на основной бирже
price_2 - текущая стоимость на бирже хеджирования
price_defference(%) - разница между ценой основной биржи и биржи хеджирования (в процентах)
result - итоговая ставка финансирования (то, сколько можно получить после вычета ставки на бирже хеджирования)
"""