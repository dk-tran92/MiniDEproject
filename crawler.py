from binance.client import Client
import numpy as np
from decimal import *
import time
from datetime import datetime
import psycopg2 as pg2
from psycopg2 import OperationalError, errorcodes, errors

conn = pg2.connect(host='localhost', database='postgres', user='khang', password='1221')


class crawlerDataBinance():
    EXCHANGE_INFO_ID = 0
    EXCHANGE_INFO_SYMBOL = 1
    EXCHANGE_INFO_MINQTY = 2
    EXCHANGE_INFO_TICKSIZE = 3
    EXCHANGE_INFO_STATUS = 4
    EXCHANGE_INFO_BASEASSET = 5
    EXCHANGE_INFO_QUOTEASSET = 6
    client = Client('api_key', 'api_secret')

    def __init__(self):
        pass
    
    def get_exchange_info_from_binance(self):
        '''
        Collect some basic data of pair of exchange: 
        '''
        market = []
        exchange_info = self.client.get_exchange_info()
        symbols = exchange_info['symbols']
        for symbol in symbols:
            temp = [symbol['symbol'],                 #Cap coin giao dich
                    symbol['filters'][2]['minQty'],   #Khoi luong giao dich nho nhat
                    symbol['filters'][0]['tickSize'], #Don vi gia nho nhat
                    symbol['status'],                 #Trang thai: TRADING/BREAK
                    symbol['baseAsset'],
                    symbol['quoteAsset']]
            market.append(temp)
        return market


    def insert_exchange_info_to_db(self):
        '''
        query INSERT INTO database exchangeInfo
        '''
        cursor = conn.cursor()
        exchange_info = self.get_exchange_info_from_binance()
        try:
            query_string = "INSERT INTO exchangeInfo(symbol, minQty, tickSize, status, baseAsset, quoteAsset) VALUES (%s,%s,%s,%s,%s,%s)"
            #pg2.extras.execute_batch(cursor, query_string, exchange_info)
            cursor.executemany(query_string, exchange_info)
            conn.commit()
        except Exception as err:
            conn.rollback()
            print(f"ZZ----Something went wrong: {err}")
        cursor.close()
        #conn.close()
        del exchange_info


    def get_exchange_info_from_db(self):
        cursor = conn.cursor()
        symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOTUSDT', 'DOGEUSDT', 'AVAXUSDT', 'SHIBUSDT',\
                   'LUNAUSDT', 'LTCUSDT', 'UNIUSDT', 'LINKUSDT', 'ALGOUSDT', 'MATICUSDT', 'BCHUSDT', 'XLMUSDT', 'AXSXUSDT', 'ICPUSDT']
        #query_string = "SELECT exchange_id, symbol, minQty, tickSize FROM exchangeInfo"
        #query_string = "SELECT exchange_id, symbol FROM exchangeInfo WHERE symbol IN ('BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'NEARUSDT')"
        query_string = "SELECT exchange_id, symbol FROM exchangeInfo\
                        WHERE symbol IN (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(query_string, symbols)
        exchanges_info = cursor.fetchall()
        cursor.close()
        #conn.close()
        return exchanges_info


    def insert_klines_data(self, klines, exchange_id): #TESTED
        cursor = conn.cursor()
        klines = np.insert(klines, [0], [exchange_id], axis=1).tolist()
        try:
            #query_string = "INSERT INTO klinesData(exchange_id, openTime, open, high, low, close, volume, closeTime, quoteAssetVolume, numberOfTrader, takerBuyBaseAssetVolume, takerBuyQuoteAssetVolume, ignore) \
            #VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            query_string = "INSERT INTO klinesData(exchange_id, openTime, open, high, low, close, volume, closeTime, quoteAssetVolume, numberOfTrader, takerBuyBaseAssetVolume, takerBuyQuoteAssetVolume, ignore) \
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            #pg2.extras.execute_batch(cursor, query_string, klines)
            cursor.executemany(query_string, klines)
            conn.commit()
        except Exception as err:
            conn.rollback()
            print(f"AA----Something went wrong: {err}")
        cursor.close()
        #conn.close()
        del klines


    def get_max_closeTime_from_db(self, exchange_id): #TESTED
        cursor = conn.cursor()
        query_string = "SELECT MAX(closeTime) FROM klinesData WHERE exchange_id = %s" % exchange_id
        cursor.execute(query_string)
        exchanges_info = cursor.fetchall()
        cursor.close()
        #conn.close()
        if exchanges_info[0][0] == None:
            return 0
        return exchanges_info[0][0]


    def get_klines_startTime(self, symbol, startTime = 0): #TESTED    
        return self.client.get_klines(symbol = symbol,
                                      interval = Client.KLINE_INTERVAL_1HOUR,
                                      startTime = startTime,
                                      limit = 1000
                                      )
    

    def insert_klines_data_to_database(self):
        symbols = self.get_exchange_info_from_db() #Query exchange info: list of (exchange_id, symbol, minQty, tickSize)
        for symbol in symbols:
            print(f'Fetching {symbol[self.EXCHANGE_INFO_SYMBOL]} ...')
            closeTime = self.get_max_closeTime_from_db(symbol[self.EXCHANGE_INFO_ID])         #get last time db
            print(f'Max close0: {closeTime}')
            if closeTime == 0:
                closeTime = 1609434000000  #    Fri Jan 01 2021 00:00:00 Asia/Ho_Chi_Minh
            klines = self.get_klines_startTime(symbol[self.EXCHANGE_INFO_SYMBOL], closeTime + 1)  #get Klines data from last time to current
            while len(klines) > 0:
                self.insert_klines_data(klines, symbol[self.EXCHANGE_INFO_ID])
                closeTime = self.get_max_closeTime_from_db(symbol[self.EXCHANGE_INFO_ID])
                print(f'Max close0: {closeTime}')
                klines = self.get_klines_startTime(symbol[self.EXCHANGE_INFO_SYMBOL], closeTime + 1)
            del klines
            print(f'Fetch {symbol[self.EXCHANGE_INFO_SYMBOL]} ... DONE!')



if __name__ == '__main__':
    time.strftime('%X %x')
    start_time = time.time()
    crawler = crawlerDataBinance()
    #crawler.insert_exchange_info_to_db() # run only in very first time
    crawler.insert_klines_data_to_database()
    



    #bb = crawler.get_exchange_info_from_db()
    #print(bb)
    #######################
    #aa = crawler.get_max_closeTime_from_db(12)
    #print(aa)
    #######################


    print("Total time get data: %f"%(time.time() - start_time))
    conn.close()