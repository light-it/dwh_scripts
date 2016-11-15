/******************************
** File: dim_numbers.sql
** Name: 
** Desc: Numbers table is frequently used in data warehousing for converting corsstab to single row results
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

DROP TABLE IF EXISTS dw.dim_numbers;

-- Create table
create table dw.dim_numbers (row integer not null, primary key (row)) DISTSTYLE EVEN sortkey (row);

-- Insert the number sequences

insert into dw.dim_numbers
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
select t1.num + 10*(t2.num-1) + 100*(t3.num-1) + 1000*(t4.num-1) + 10000*(t5.num-1) as row
from ten t1
 cross join ten t2
 cross join ten t3
 cross join ten t4
 cross join ten t5
order by 1
;