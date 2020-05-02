DROP TABLE if exists jAll;
DROP TABLE if exists richiestaA;
DROP TABLE if exists minMaxDate;
DROP TABLE if exists firstClose;
DROP TABLE if exists lastClose;
DROP TABLE if exists variazioneAnnualeMedia;




CREATE TABLE jAll AS
SELECT  HSP.ticker,HS.sector,HS.name,HSP.volume,HSP.close,HSP.year,to_date(concat(HSP.year,'-',HSP.month,'-',HSP.day)) as data 
FROM stock_prices_byYear HSP JOIN historicalStocks HS ON(HSP.ticker=HS.ticker);

CREATE TABLE richiestaA AS
SELECT  pr.sector, pr.year, AVG(sommaVolume) AS volumeAnnuleMedio FROM
 (SELECT  total.sector AS  sector ,count(total.volume) AS sommaVolume, total.year AS year FROM
   (SELECT DISTINCT sector,volume,year,ticker
    FROM jAll) AS total
  GROUP BY total.sector,total.ticker,total.year) AS pr
GROUP BY pr.sector,pr.year;


CREATE TABLE minMaxDate AS
SELECT ticker, name, sector, year, MIN(data) as datamenorecente, MAX(data) as datapiurecente
FROM jAll
GROUP BY ticker, name, year, sector;

CREATE TABLE firstClose AS
SELECT DISTINCT D.ticker,D.sector, M.name, D.close, M.year
FROM jAll D  JOIN  minMaxDate M ON(D.ticker = M.ticker AND  D.data = M.datamenorecente AND D.name=M.name AND D.year=M.year AND D.sector=M.sector);

CREATE TABLE lastClose AS
SELECT DISTINCT D.ticker,D.sector, M.name, D.close, M.year
FROM jAll D JOIN  minMaxDate M ON(D.ticker = M.ticker AND  D.data = M.datapiurecente AND D.name=M.name AND D.sector=M.sector);

CREATE TABLE variazioneAnnualeMedia AS
SELECT va.sector, va.year, AVG(variazione) AS varazioneAnnuleMedia FROM
  (SELECT DISTINCT FC.ticker, FC.sector, FC.name, FC.year, round((LC.close/FC.close)*100-100,0) AS variazione
   FROM firstClose FC JOIN lastClose LC ON(FC.ticker=LC.ticker AND FC.name=LC.name AND FC.year=LC.year AND FC.sector=LC.sector))AS va
GROUP BY va.sector, va.year; 
