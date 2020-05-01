DROP TABLE if exists richiestaA;
DROP TABLE if exists jointable

CREATE TABLE jointable AS
SELECT sector,volume,year,ticker,close,year,month,day
FROM historical_stock_prices_byYear HSP JOIN historicalStocks HS ON(HSP.ticker=HS.ticker); 


CREATE TABLE richiestaA AS 
SELECT pr.sector, AVG(sommaVolume),pr.year FROM
 (SELECT total.sector as sector ,count(total.volume) AS sommaVolume, total.year as year FROM
   (SELECT sector,volume,year,ticker 
    FROM jointable)AS total
  GROUP BY total.sector,total.ticker)AS pr


