# MiniDEproject
## Crawler Exchange information from Binance  
Python scritp to:  
\tInteractive with PostgresSQL database by psycopg2  
  CreateDB: 2 table: exchangeinfo (exchange metadata), klinesdata (candle stick data for some exchange pair)  
  Request data from BinanceAPI then INSERT directly to PostgresSQL  
  Check the last time of exchange info in postgres if it is up-to-date or not, then request the lack data only  
  This script should run hourly to get KLINE_INTERVAL_1HOUR data   
