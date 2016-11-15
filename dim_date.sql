/******************************
** File: dim_date.sql
** Name: 
** Desc: Date Dimenstion creaton script for Amazon Redshift.
** Auth: Kirill Andriychuk  Key2Market.com
** Date: 16/11/2016
**************************
** Change History
**************************
** PR   Date        Author  Description 
** --   --------    ------- ------------------------------------
** 1    16/11/2016  Kirill  Crated 1st version of the SQL script
*******************************/


-- Drope the table if it exists

DROP TABLE IF EXISTS dw.dim_date;

-- Create a new table

CREATE TABLE dw.dim_date (
  id BIGINT
  ,date_dt DATE
  ,unix BIGINT
  ,weekend varchar(10) DEFAULT 'Weekday'
  ,dow_num smallint -- Day of Week Numeric. Mon=1, Sun=7
  ,dow_name varchar(16) -- Day of Week Name
  ,doy_num int -- Day of Year Numeric
  ,month_num smallint
  ,day_num smallint
  ,year_num INT
  ,week_num_start_sun int -- Week number of week starts on Sundays
  ,week_num_start_mon int -- Week number of week starts on Mondays
  ,week_start_sun_min_dt date -- First date of the week if week starts on Sundays
  ,week_start_sun_max_dt date -- Last date of the week if week starts on Sundays
  ,week_start_mon_min_dt date -- First date of the week if week starts on Mondays
  ,week_start_mon_max_dt date -- Last date of the week if week starts on Mondays
)
-- distkey(id) -- Redshift
;

-- Populate the dates into the new table

INSERT INTO dw.dim_date (id, date_dt, unix, dow_num, dow_name, doy_num, weekend, day_num, month_num, year_num, week_num_start_sun, week_num_start_mon
,week_start_sun_min_dt, week_start_sun_max_dt, week_start_mon_min_dt, week_start_mon_max_dt
)
-- generate sequence of numbers
with ten as (
 select 1 as num
 union all select 2
 union all select 3
 union all select 4
 union all select 5
 union all select 6
 union all select 7
 union all select 8
 union all select 9
 union all select 10
)

SELECT row
, date_dt
, DATE_PART('epoch',date_dt)
, CASE WHEN DATE_PART('dow',date_dt) = 0 THEN 7 ELSE DATE_PART('dow',date_dt) END
, to_char(date_dt,'Day')
, DATE_PART('doy',date_dt)
, CASE WHEN DATE_PART('dow',date_dt) IN (5,6) THEN 'Weekend' ELSE 'Weekday' END
, DATE_PART( 'day',date_dt)
, DATE_PART( 'mon',date_dt)
, DATE_PART( 'y',date_dt)
, DATE_PART('week',date_dt + INTERVAL '1 day')
, DATE_PART('week',date_dt)
, MIN(date_dt) OVER (PARTITION BY DATE_PART('week',date_dt + INTERVAL '1 day'),DATE_PART( 'y',date_dt) ) 
, MAX(date_dt) OVER (PARTITION BY DATE_PART('week',date_dt + INTERVAL '1 day'),DATE_PART( 'y',date_dt) ) 
, MIN(date_dt) OVER (PARTITION BY DATE_PART('week',date_dt),DATE_PART( 'y',date_dt) ) 
, MAX(date_dt) OVER (PARTITION BY DATE_PART('week',date_dt),DATE_PART( 'y',date_dt) ) 
FROM
(
	SELECT row
	--, DATE_ADD('day',row,'2010-01-01' ) date_dt -- Redshift
	, '2009-12-31'::date + INTERVAL '1 day' * row date_dt -- Postgres
	  FROM 
	  (
	  	-- Select sequences of numbers from which we will subsequently create dates
	  	select t1.num + 10*(t2.num-1) + 100*(t3.num-1) + 1000*(t4.num-1) + 10000*(t5.num-1) as row
		from ten t1
		 cross join ten t2
		 cross join ten t3
		 cross join ten t4
		 cross join ten t5
		order by 1
	  ) t1
	  -- WHERE  DATE_ADD('day', row,'2010-01-01' ) BETWEEN '2010-01-01' AND '2050-01-01'  -- Redshift
	WHERE ('2009-12-31'::date + INTERVAL '1 day' * row) BETWEEN '2010-01-01' AND '2050-01-01' -- Postgres
) t2
ORDER BY row
;


VACUUM dw.dim_date;
ANALYZE dw.dim_date;
