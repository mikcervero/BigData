DROP TABLE if exists jAll;
DROP TABLE if exists richiestaA;

CREATE TABLE jAll AS
SELECT HSP.ticker,HS.sector,HSP.volume,HSP.close,HSP.day,HSP.month,HSP.year
FROM stock_prices_byYear HSP JOIN historicalStocks HS ON(HSP.ticker=HS.ticker);


SELECT pr.sector, pr.year, AVG(sommaVolume) AS volumeAnnuleMedio FROM
 (SELECT total.sector AS  sector ,count(total.volume) AS sommaVolume, total.year AS year FROM
   (SELECT sector,volume,year,ticker
    FROM jAll) AS total
  GROUP BY total.sector,total.ticker,total.year) AS pr
GROUP BY pr.sector,pr.year;



