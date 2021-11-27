import psycopg2 as pg2
conn = pg2.connect(host='localhost', database='postgres', user='khang',password='1221')
cur = conn.cursor()


#Query to create table
query_drop_table_exchangeInfo = '''DROP TABLE IF EXISTS exchangeInfo;'''
query_create_table_exchangeInfo = '''
CREATE TABLE exchangeinfo (
  exchange_id SERIAL PRIMARY KEY,
  symbol VARCHAR(20) UNIQUE NOT NULL,
  minQty DECIMAL(15,10) NOT NULL,
  tickSize DECIMAL(15,10) NOT NULL,
  status VARCHAR(20) NOT NULL,
  baseAsset VARCHAR(10) NOT NULL,
  quoteAsset VARCHAR(10) NOT NULL
);'''



query_create_table_klinesData = '''
DROP TABLE IF EXISTS klinesdata;
CREATE TABLE klinesdata (
  k_line_id SERIAL PRIMARY KEY,
  exchange_id SMALLINT REFERENCES exchangeinfo(exchange_id),
  openTime BIGINT NOT NULL,
  open DECIMAL NOT NULL,
  high DECIMAL NOT NULL,
  low DECIMAL NOT NULL,
  close DECIMAL NOT NULL,
  volume DECIMAL NOT NULL,
  closeTime BIGINT NOT NULL,
  quoteAssetVolume DECIMAL NOT NULL,
  numberOfTrader INT NOT NULL,
  takerBuyBaseAssetVolume DECIMAL NOT NULL,
  takerBuyQuoteAssetVolume DECIMAL NOT NULL,
  ignore DECIMAL NOT NULL
  );'''


#Run query
cur.execute(query_drop_table_exchangeInfo)
cur.execute(query_create_table_exchangeInfo)
cur.execute(query_create_table_klinesData)


#Commit the change to database
conn.commit()


#Close connection
cur.close()
conn.close()
