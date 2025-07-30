
--creating a new table with unique country names
create table country_names as (
select distinct country from confirmed_cases);

--change column nemes from confirmed_cases to cases and confirmed_deaths to deaths
alter table confirmed_cases 
rename column confirmed_cases to cases;

alter table confirmed_deaths 
rename column confirmed_deaths to deaths;


-- Total confirmed cases per country

/* since this data is cumulative, ie every cases is added up to yesterday's
case, we will only look for the case recorded on the latest day per country */

select c.country, latest.latest_date, c.cases 
from confirmed_cases c
join (
	select country, max(date) as latest_date
	from confirmed_cases
	group by country
) latest
on c.country  = latest.country and c.date  = latest.latest_date
order by c.cases desc;

-- Getting the daily new cases per country
select country, date,
	cases - lag(cases, 1, 0) over (
	partition by country
	order by date) as new_cases
from confirmed_cases;

--Getting the first reported case date per country

select country, min(date) as first_case_date
from confirmed_cases 
where cases > 0
group by country;

--Getting how many cases were reported on the first date per country

select c.country, first.first_case_date, c.cases
from confirmed_cases c
join (
	 select country, min(date) as first_case_date
	 from confirmed_cases 
	 where cases > 0
	 group by country
) first
on c.country = first.country and c.date = first.first_case_date
order by cases desc;

--countries with highest cases
select country, max(cases) as highest_cases
from confirmed_cases
group by country 
order by highest_cases desc
limit 10;

-- weekly report per country
with daily_new_cases as (
	select country,
	date::date,
		cases - lag(cases, 1, 0) over (
		partition by country
		order by date) as new_cases
	from confirmed_cases)
SELECT
    country,
    date_trunc('week', date) AS week_start,
    sum(new_cases) AS weekly_new_cases
  FROM daily_new_cases
  GROUP BY country, DATE_TRUNC('week', date);





