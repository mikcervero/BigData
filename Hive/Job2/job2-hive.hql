DROP TABLE if exists jAll;
DROP TABLE if exists richiestaA;
DROP TABLE if exists minMaxDate;
DROP TABLE if exists firstClose;
DROP TABLE if exists lastClose;
DROP TABLE if exists variazioneAnnualeM;
DROP TABLE if exists quotazioneGiornalieraM;
DROP TABLE if exists job2;


CREATE TABLE jAll AS
SELECT  HSP.ticker,HS.sector,HSP.volume,HSP.close,HSP.year,to_date(concat(HSP.year,'-',HSP.month,'-',HSP.day)) as data 
FROM stock_prices_byYear HSP JOIN historicalStocks HS ON(HSP.ticker=HS.ticker);

CREATE TABLE  richiestaA AS
SELECT  pr.sector, pr.year, AVG(sommaVolume) AS volumeAnnuleMedio FROM
 (SELECT  total.sector AS  sector ,SUM(total.volume) AS sommaVolume, total.year AS year FROM
   (SELECT sector,volume,year,ticker
    FROM jAll) AS total
  GROUP BY total.sector,total.ticker,total.year) AS pr
GROUP BY pr.sector,pr.year;


CREATE TABLE IF NOT EXISTS minMaxDate AS
SELECT ticker, sector, year, MIN(data) as datamenorecente, MAX(data) as datapiurecente
FROM jAll
GROUP BY ticker, year, sector;

CREATE TABLE IF NOT EXISTS firstClose AS
SELECT  D.ticker,D.sector, D.close, M.year
FROM jAll D  JOIN  minMaxDate M ON(D.ticker = M.ticker AND  D.data = M.datamenorecente  AND D.year=M.year AND D.sector=M.sector);

CREATE TABLE IF NOT EXISTS lastClose AS
SELECT  D.ticker,D.sector, D.close, M.year
FROM jAll D JOIN  minMaxDate M ON(D.ticker = M.ticker AND  D.data = M.datapiurecente AND D.sector=M.sector);

CREATE TABLE IF NOT EXISTS variazioneAnnualeM AS
SELECT va.sector, va.year, AVG(variazione) AS varazioneAnnualeMedia FROM
  (SELECT FC.ticker, FC.sector, FC.year, round((LC.close/FC.close)*100-100,0) AS variazione
   FROM firstClose FC JOIN lastClose LC ON(FC.ticker=LC.ticker AND FC.year=LC.year AND FC.sector=LC.sector)) AS va
GROUP BY va.sector, va.year; 

CREATE TABLE IF NOT EXISTS quotazioneGiornalieraM AS
SELECT va.sector,va.year, AVG(quotazioneGiornalieraMediaA) AS quotazioneGiornalieraMediaS FROM
   (SELECT sector,year,ticker, AVG(close) AS quotazioneGiornalieraMediaA 
    FROM jAll
    GROUP BY sector,year,ticker) AS va
GROUP BY va.sector,va.year; 

CREATE TABLE IF NOT EXISTS job2  AS
SELECT VA.sector,VA.year,R.volumeAnnuleMedio,VA.varazioneAnnualeMedia,QA.quotazioneGiornalieraMediaS
FROM richiestaA R JOIN variazioneAnnualeM VA ON(R.sector=VA.sector AND R.year=VA.year) JOIN quotazioneGiornalieraM QA ON(VA.sector=QA.sector AND VA.year=QA.year);

