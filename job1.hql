drop table if exists job1;

--impiega 143 secondi
create table job1 as
select ticker, round((sum(close)/sum(open))*100-100,0) as quotazione, MIN (lowThe) as prezzo_minimo, MAX (highThe) as prezzo_massimo, AVG(volume) as media_volume
from stock_prices_byYear
group by ticker
order by quotazione DESC;

--stessa query senza utilizzare partizioni richiede 204 secondi
--select ticker, (sum(close)/sum(open))*100-100 as quotazione, MIN (lowThe), MAX (highThe), AVG(volume)
--from tmp
--where year>=2008 and year<=2018
--group by ticker
--order by quotazione DESC;
