drop table if exists triennio;

create table triennio as 
select *
from jall
where year(data)>=2016;

drop table if exists intermediate;

create table intermediate as
select ticker, name, MIN(data) as datamenorecente, MAX(data) as datapiurecente, year(data) as anno
from triennio
group by ticker, name, year(data);

drop table if exists chiusurainiziale;

create table chiusurainiziale as
select t.ticker, t.name, close, i.anno
from triennio t join intermediate i on t.ticker = i.ticker and t.name = i.name and t.data = i.datamenorecente and year(t.data) = i.anno;

drop table if exists chiusurafinale;

create table chiusurafinale as
select t.ticker, t.name, close, i.anno
from triennio t join intermediate i on t.ticker = i.ticker and t.name = i.name and t.data = i.datapiurecente and year(t.data) = i.anno;

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
from intermediate2 group by quotazione



