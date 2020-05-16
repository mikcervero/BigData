DROP TABLE if exists historicalStockPrices;
DROP TABLE if exists jAll;


CREATE TABLE  historicalStockPrices (ticker STRING, open DOUBLE, close DOUBLE, adj_close DOUBLE, lowThe DOUBLE, highThe DOUBLE, volume INT, data DATE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/Applications/GitHub/BigData/DataSet/500000-historical_stock_prices.csv' OVERWRITE INTO TABLE historicalStockPrices;

DROP TABLE if exists historicalStocksRow;

CREATE TABLE historicalStocksRow(stockRow STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\n";

LOAD DATA LOCAL INPATH '/Users/micol/Downloads/daily-historical-stock-prices-1970-2018/historical_stocks.csv' OVERWRITE INTO TABLE historicalStocksRow;

add jar /Applications/GitHub/BigData/Hive/Job3/Parser.jar ;                                   
CREATE TEMPORARY FUNCTION Parser AS 'Hive.Parser.Parser';


DROP TABLE if exists historicalStocks;

CREATE TABLE historicalStocks as 
SELECT Parser(stockRow) as (ticker,name)
FROM historicalStocksRow;


CREATE TABLE jAll AS
SELECT  HSP.ticker,HS.name,HSP.close,year(data) as year, HSP.data 
FROM historicalStockPrices HSP JOIN historicalStocks HS ON(HSP.ticker=HS.ticker)
WHERE year(data)>=2016 AND year(data)<=2018;

drop table if exists intermediate;

create table intermediate as
select ticker, name, MIN(data) as datamenorecente, MAX(data) as datapiurecente, year(data) as anno
from jAll
group by ticker, name, year(data);

drop table if exists chiusurainiziale;

create table chiusurainiziale as
select t.ticker, t.name, close, i.anno
from jAll t join intermediate i on t.ticker = i.ticker and t.name = i.name and t.data = i.datamenorecente and year(t.data) = i.anno;

drop table if exists chiusurafinale;

create table chiusurafinale as
select t.ticker, t.name, close, i.anno
from jAll t join intermediate i on t.ticker = i.ticker and t.name = i.name and t.data = i.datapiurecente and year(t.data) = i.anno;

drop table if exists ordinatianno;
create table ordinatianno as
select * from 
		(select ci.ticker as ticker ,ci.name as nome, round((cf.close/ci.close)*100-100,0) as quotazione, ci.anno as anno
		from chiusurainiziale ci join chiusurafinale cf on ci.ticker = cf. ticker and ci.name = cf.name and ci.anno = cf.anno) as a
order by a.anno;

drop table if exists intermediate2;
create table intermediate2 as
select ticker, nome, concat_ws(',',collect_list(cast (quotazione as string))) as quotazione
from ordinatianno
group by ticker, nome;

drop table if exists job3;
create table job3 as 
select collect_set(nome), quotazione 
from intermediate2 
group by quotazione
where size(collect_set(nome))>1;

