drop table if exists data_stock_prices_byyear;
drop table if exists stock_prices;

create table stock_prices (ticker STRING, open DOUBLE, close DOUBLE, adj_close DOUBLE, lowThe DOUBLE, highThe DOUBLE, volume INT, data DATE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/path/historical_stock_prices.csv' OVERWRITE INTO TABLE stock_prices;


create table data_stock_prices_byyear as
SELECT ticker, close, volume, data
from stock_prices
where year(data)>=2008 AND year(data)<=2018;

drop table if exists intermediate;

create table intermediate as
select ticker, MIN(data) as datamenorecente, MAX(data) as datapiurecente
from data_stock_prices_byyear
group by ticker;

drop table if exists chiusurainiziale;

create table chiusurainiziale as
select d.ticker, close
from data_stock_prices_byyear d join intermediate i on d.ticker = i.ticker and d.data = i.datamenorecente;

drop table if exists chiusurafinale;

create table chiusurafinale as
select d.ticker, close
from data_stock_prices_byyear d join intermediate i on d.ticker = i.ticker and d.data = i.datapiurecente;

drop table if exists intermediate2;

create table intermediate2 as
select ticker, MIN(close) as prezzominimo, MAX(close) as prezzomassimo, AVG(volume) as mediavolume
from data_stock_prices_byyear
group by ticker;

drop table if exists job1;

create table job1 as
select ii.ticker, round((cf.close/ci.close)*100-100,0) as quotazione, prezzominimo, prezzomassimo, mediavolume
from chiusurainiziale ci join chiusurafinale cf on ci.ticker = cf.ticker join intermediate2 ii on ii.ticker = ci.ticker
order by quotazione DESC;


