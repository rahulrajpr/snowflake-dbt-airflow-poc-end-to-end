
select current_database()

---

create schema poc_schema

set schema 'poc_schema';

--

select current_schema()

---

drop table if exists poc_schema.poc_table;

create table if not exists poc_schema.poc_table
as 
(
with recursive cte(num) as 
(
select 1 as num 
union all
select num + 1
from cte 
where num < 10
)
select *,
	num*num as sq,
	(num*num) - num as var
from cte
);

---

select *
from poc_schema.poc_table

