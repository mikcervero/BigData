DROP TABLE if exists historicalStockPrices;

CREATE TABLE historicalStockPrices (ticker STRING, open DOUBLE, close DOUBLE, adj_close DOUBLE, lowThe DOUBLE, highThe DOUBLE, volume INT, data DATE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/Users/micol/Downloads/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv' OVERWRITE INTO TABLE historicalStockPrices;


CREATE TABLE historicalStocks (ticker STRING, exchange STRING, nome STRING, sector STRING, industry STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ("input.regex" = "[",]") stored as textfile;

LOAD DATA LOCAL INPATH '/Users/micol/Downloads/daily-historical-stock-prices-1970-2018/historical_stocks.csv' OVERWRITE INTO TABLE historicalStocks;

DROP TABLE if exists tmp; 

CREATE TABLE tmp as
SELECT ticker, close, volume, substr(data,1,4) as year, substr (data,6,2) as month, substr (data,9,2) as day
FROM historicalStockPrices;

DROP TABLE if exists historical_stock_prices_byYear;

CREATE TABLE historical_stock_prices_byYear (ticker STRING, close DOUBLE, volume INT, month INT, day INT) PARTITIONED BY (year INT);

INSERT OVERWRITE TABLE historical_stock_prices_byYear PARTITION (year=2008) SELECT ticker, close, volume, month, day FROM tmp where year='2008';

INSERT OVERWRITE TABLE historical_stock_prices_byYear PARTITION (year=2009) SELECT ticker, close, volume, month, day FROM tmp where year='2009';

INSERT OVERWRITE TABLE historical_stock_prices_byYear PARTITION (year=2010) SELECT ticker, close, volume, month, day FROM tmp where year='2010';

INSERT OVERWRITE TABLE historical_ stock_prices_byYear PARTITION (year=2011) SELECT ticker, close, volume, month, day FROM tmp where year='2011';

INSERT OVERWRITE TABLE historical_stock_prices_byYear PARTITION (year=2012) SELECT ticker, close, volume, month, day FROM tmp where year='2012';

INSERT OVERWRITE TABLE historical_stock_prices_byYear PARTITION (year=2013) SELECT ticker, close, volume, month, day FROM tmp where year='2013';

INSERT OVERWRITE TABLE historical_stock_prices_byYear PARTITION (year=2014) SELECT ticker, close, volume, month, day FROM tmp where year='2014';

INSERT OVERWRITE TABLE historical_stock_prices_byYear PARTITION (year=2015) SELECT ticker, close, volume, month, day FROM tmp where year='2015';

INSERT OVERWRITE TABLE historical_stock_prices_byYear PARTITION (year=2016) SELECT ticker, close, volume, month, day FROM tmp where year='2016';

INSERT OVERWRITE TABLE historical_stock_prices_byYear PARTITION (year=2017) SELECT ticker, close, volume, month, day FROM tmp where year='2017';

INSERT OVERWRITE TABLE historical_stock_prices_byYear PARTITION (year=2018) SELECT ticker, close, volume, month, day FROM tmp where year='2018';

