--příprava na sjednocení tabulek price a payroll prostřednictvím view
--vybrání průměrných cen potravin přes celou ČR (NULL) a sjednocení časového období s payroll na rok a čtvtletí

create or replace view v_karolina_machova_dvorakova_price as
select
	avg(value) as price,
	category_code as product_code,
	date_part('YEAR', date_from) as price_year,
	date_part('QUARTER', date_from) as price_quarter
from czechia_price cp 
where region_code is null
group by price_year, price_quarter, category_code;

--drop table t_karolina_dvorakovamachova_project_SQL_primary_final;
create table t_karolina_dvorakovamachova_project_SQL_primary_final as
select *
from czechia_payroll cp 
left join v_karolina_machova_dvorakova_price vp
	on vp.price_year = cp.payroll_year
	and vp.price_quarter = cp.payroll_quarter;
select 
count(1)
from "t_karolina_dvorakovamachova_project_SQL_primary_final" tkdpspf;

--první výzkumná otázka
with rostouci_mzdy as(
	select
		payroll_year as payroll_year,
		industry_branch_code as ibc,
    	round(avg(value),2) as payroll,
    	lag (round(avg(value),2)) over (partition by industry_branch_code order by avg(value)) as payroll_ly
	from t_karolina_dvorakovamachova_project_SQL_primary_final as tkdpspf 
		where value_type_code = 5958
		and unit_code = 200
		and industry_branch_code is not null
	group by industry_branch_code, payroll_year
	order by industry_branch_code, payroll_year
)
select 
	distinct ibc
from rostouci_mzdy
	where payroll > payroll_ly
group by ibc
 having count(payroll) = 21; --první rok porovnávat nemusíme

 
 --druhá výzkumná otázka

WITH extreme_values AS (
    select 
        MIN(price_year) AS min_year,
        MAX(price_year) AS max_year,
        MIN(price_quarter) AS min_quarter,
        MAX(price_quarter) AS max_quarter
    from t_karolina_dvorakovamachova_project_SQL_primary_final
    where value_type_code = 5958
    and unit_code = 200
    and price is not null
    and product_code in (114201, 111301)
   )
select
	price_year as price_year,
	price_quarter as price_quarter,
	avg(price) as avg_price,
	avg(value) as avg_payroll,
	avg(value)/avg(price) as pieces,
	product_code as pc
from t_karolina_dvorakovamachova_project_SQL_primary_final as tkdpspf
join extreme_values ev 
    on (tkdpspf.price_year = ev.max_year and tkdpspf.price_quarter = ev.max_quarter)
    or (tkdpspf.price_year = ev.min_year and tkdpspf.price_quarter = ev.min_quarter)
where value_type_code = 5958
and unit_code = 200
and price is not null
and product_code in (114201, 111301)
group by price_year, price_quarter, product_code;


-- třetí výzkumná otázka
-- nejpomaleji dražuje = nejmenší procentuální rozdíl v průměrné ceně za první a poslední sledovaný rok (to už víme, že je 2006 a 2018)
with minimal_rise as(
	select
		avg(price) as avg_price,
		product_code as pc,
		price_year as price_year,
		lag(avg(price),1) over (partition by product_code order by price_year)  as lag_price,
		((avg(price)/(lag(avg(price),1) over (partition by product_code order by price_year)) ) -1)*100 as perc_dif
	from "t_karolina_dvorakovamachova_project_SQL_primary_final" as tkdpspf
	where price_year in (2006, 2018)
	group by price_year, product_code
	order by pc asc
	)
select*
from minimal_rise 
order by perc_dif asc
limit 1
	;

-- Cukr krystal má nejmenší nárůst ceny (záporný). Oproti roku 2006 byl v roce 2018 dokonce o 27,5 procent levnější

-- čtvrtá výzkumná otázka

with rise_less_10 as(
	select
		avg(price) as avg_price,
		price_year as price_year,
		lag(avg(price),1 )over (order by price_year)  as lag_price,
		(avg(price)/(lag(avg(price),1) over (order by price_year)) -1)*100 as perc_dif
	from "t_karolina_dvorakovamachova_project_SQL_primary_final" as tkdpspf
	group by price_year
	order by price_year  asc
	)
select*
from rise_less_10
where perc_dif > 10
order by price_year asc
;
-- neexistuje

--pátá výzkumná otázka
--tvorba dvou view z tabulek economies a primární tabulky t_karolina_dvorakovamachova_project_SQL_primary_final jako příprava na sloučení

--drop view v_karolina_machova_dvorakova_country;
create or replace view v_karolina_machova_dvorakova_country as
select
	country as country,
	gdp/population as hdp_per_cap,
	year as year_country
from economies ec 
where country = 'Czech Republic';


--drop view v_karolina_machova_dvorakova_payroll_year;

create or replace view v_karolina_machova_dvorakova_payroll_year as
select price_year as price_year,
	avg(price) as price,
	avg(value) as payroll
from t_karolina_dvorakovamachova_project_SQL_primary_final prim
where value_type_code = 5958
	and unit_code = 200
	and price_year is not null
group by price_year;

--drop table t_karolina_dvorakovamachova_project_SQL_secondary_final;

create table t_karolina_dvorakovamachova_project_SQL_secondary_final as
select *,
	hdp_per_cap - (lag(hdp_per_cap, 1) over (order by price_year)) as delta_hdp_pc,
	((lead(payroll, 1) over (order by price_year)) - payroll) as delta_payroll_1, --zvýšení na další rok
	((lead(price, 1) over (order by price_year)) - price) as delta_price_1,
	payroll - (lag(payroll, 1) over (order by price_year))  as delta_payroll_0, --zvýšení na ten rok
	price - (lag(price, 1) over (order by price_year))  as delta_price_0 
from v_karolina_machova_dvorakova_country
right join v_karolina_machova_dvorakova_payroll_year on (v_karolina_machova_dvorakova_payroll_year.price_year =  v_karolina_machova_dvorakova_country.year_country)
order by price_year asc;

select *
from t_karolina_dvorakovamachova_project_SQL_secondary_final
where delta_hdp_pc >= 1000;
