SELECT
    Region,
    Product,
    SalesAmount,
    ROW_NUMBER() OVER (PARTITION BY Region ORDER BY SalesAmount) AS RowNumber,
    RANK() OVER (PARTITION BY Region ORDER BY SalesAmount) AS Rank,
    DENSE_RANK() OVER (PARTITION BY Region ORDER BY SalesAmount) AS DenseRank,
    NTILE(2) OVER (PARTITION BY Region ORDER BY SalesAmount) AS NTile,
    LEAD(Product) OVER (PARTITION BY Region ORDER BY SalesAmount) AS LeadProduct,
    LAG(Product) OVER (PARTITION BY Region ORDER BY SalesAmount) AS LagProduct,
    COUNT(*) OVER (PARTITION BY Region) AS CountByRegion,
    SUM(SalesAmount) OVER (PARTITION BY Region) AS SumByRegion,
    AVG(SalesAmount) OVER (PARTITION BY Region) AS AvgByRegion,
    MIN(SalesAmount) OVER (PARTITION BY Region) AS MinByRegion,
    MAX(SalesAmount) OVER (PARTITION BY Region) AS MaxByRegion
FROM
    Sales;

select user_id, user_name, email
from (
select *,
row_number() over (partition by user_name order by user_id) as rn
from users u
order by user_id) x
where x.rn <> 1;
--paritioned on username to get duplicate records and then we used to to find more than 1

-- Solution:
select emp_id, emp_name, dept_name, salary
from (
select *,
row_number() over (order by emp_id desc) as rn
from employee e) x
where x.rn = 2;
 -- partion by wasnt used and stratigt order by to give each row number an individual number

select x.*
from employee e
join (select *,
max(salary) over (partition by dept_name) as max_salary,
min(salary) over (partition by dept_name) as min_salary
from employee) x
on e.emp_id = x.emp_id
and (e.salary = x.max_salary or e.salary = x.min_salary)
order by x.dept_name, x.salary;


--separated columns created and then we used that to subquery 


<>><this is self join to find doctors working in same hopspital different specialty 

select a.* from doctors a
join doctors b on a.id<>b.id
and a.hospital=b.hospital and a.speciality<>b.speciality

---- we joined the same 

 select distinct user_name from (select *,
 case when user_name = lead(user_name) over (order by login_id) 
	and user_name = lead(user_name,2) over (order by login_id)
	then user_name
	else null
	end as repeat_users
 from login_details
 ) a where a.repeat_users is not null

 select user_id , user_name , email 
from (
	select *,
	row_number() over (partition by user_name order by user_id) as rn 
	from users
) as x
where x.rn > 1