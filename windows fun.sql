use suneet;


create table city(
new_id int , 
new_cat varchar(20)
);


insert into city
values (100 , "agni"),
(200 , "agni"),
(500 , "dharti"),
(700 , "dharti"),
(200 , "vayu"),
(300 , "vayu"),
(500 , "vayu");

select * from city;

select new_id , new_cat , sum(new_id) over(partition by new_cat) as "total" from city;

select new_id , new_cat , sum(new_id) over(partition by new_cat order by new_id) as "total" from city;

select new_id , new_cat , sum(new_id) over(partition by new_cat order by new_id) as "total" 
,avg(new_id) over(partition by new_cat order by new_id) as "total" from city;


select new_id , new_cat , 
min(new_id) over(partition by new_cat order by new_id) as "min"  from city;


select new_id , new_cat , 
max(new_id) over(partition by new_cat order by new_id) as "max"  from city;


select new_id , new_cat , 
count(new_id) over(partition by new_cat order by new_id) as "count"  from city;

select new_id , new_cat , 
sum(new_id) over( order by new_id rows between unbounded preceding and unbounded following) as "total" from city;

# ranking func

select new_id , row_number() over( order by new_id ) as "row" from city;

select new_id , row_number() over( order by new_id ) as "row" ,
rank() over(order by new_id) as "rank"
from city;

select new_id , row_number() over( order by new_id ) as "row" ,
rank() over(order by new_id) as "rank",
dense_rank() over(order by new_id) as "desen"
from city;

select new_id , row_number() over( order by new_id ) as "row" ,
rank() over(order by new_id) as "rank",
dense_rank() over(order by new_id) as "desen",
percent_rank() over(order by new_id) as "percent"
from city;

select new_id , first_value(new_id) over( order by new_id ) as "fist_value" , 
last_value(new_id) over(order by new_id) as "last" , 
lead(new_id) over( order by new_id ) as "lead" , 
lag(new_id) over(order by new_id) as "lag" from city;





CREATE TABLE Sales (
    SaleID INT,
    SaleDate DATE,
    CustomerID INT,
    ProductID INT,
    Amount DECIMAL(10, 2)
);



INSERT INTO Sales (SaleID, SaleDate, CustomerID, ProductID, Amount) VALUES
(1, '2024-01-01', 101, 1001, 150.00),
(2, '2024-01-02', 102, 1002, 200.00),
(3, '2024-01-03', 101, 1001, 100.00),
(4, '2024-01-04', 103, 1003, 300.00),
(5, '2024-01-05', 102, 1002, 250.00),
(6, '2024-01-06', 101, 1001, 175.00),
(7, '2024-01-07', 104, 1004, 400.00),
(8, '2024-01-08', 105, 1005, 350.00),
(9, '2024-01-09', 102, 1002, 225.00),
(10, '2024-01-10', 101, 1001, 125.00);

INSERT INTO Sales (SaleID, SaleDate, CustomerID, ProductID, Amount) VALUES
(11, '2024-01-06', 101, 1001, 175.00);


#Question 1: Calculate the running total of sales amount.

select SaleId , ProductId , Amount  ,
sum(Amount) over(order by SaleDate) as runingTotal from Sales;

#Question 2: Calculate the average sales amount over the last 3 sales.
 
select SaleId , CustomerId , Amount , SaleDate,
avg(Amount) over(order by SaleDate rows between 2 preceding and current row) as "avg2 values" 
from sales;
 
 
# Question 3: Rank the sales by amount for each customer.

select SaleId , CustomerId , Amount , 
rank() over(partition by CustomerId order by Amount desc) as "rank"
from Sales;
 
#Question 4: Calculate the cumulative distribution of sales amount.

select SaleId , SaleDate , Amount , 
cume_dist()  over (order by Amount ) as "cumulative distribution" 
from Sales;

#Question 5: Calculate the difference in sales amount between the current sale and the previous sale.

select saleId , SaleDate , Amount , 
Amount - LAG(Amount , 1) over(order by SaleDate) as " Amount Ddiifrence" from Sales;


#Question 6: Calculate the lead sales amount for the next sale.

select saleId , SaleDate , Amount , 
lead(Amount) over(order by SaleDate) as "next sales"
from Sales;

#Question 7: Find the first sale amount for each customer. 

select SaleId , CustomerId , Amount  , SaleDate,
first_value(Amount) over(partition by customerID ) as "first value"
from Sales;

select SaleId , CustomerId , Amount  , SaleDate,
first_value(Amount) over(partition by customerID order by SaleDate) as "first value"
from Sales;



#####  view 

create table dep (
empid int ,
name varchar(20),
dept varchar(20) , 
salary int  
);


insert into dep (empid , name , dept , salary) values
(1,	"Alice",	"HR",	50000),
(2,	"Bob",	"IT"	,70000),
(3,	"Charlie" ,	"IT",	60000),
(4,	"David"	,"HR",	55000),
(5,	"Eve" ,	"Finance",	65000
);
 
 
#Create a view showing employee names and salaries only.

create view View_salary as
select name , dept , salary from dep;

select * from view_salary;    # TO CHECK THE VALUE OR CHECK VALUE IN SCHEMAS 


#View of high-earning employees (salary > 60000).  

create view high_earning as 
select name , dept , salary 
from dep where salary > 60000;



#Create a view that shows total salary by department. 

create view deptment_salary as 
select dept, sum(salary) as "total salary"
from dep group by dept;


#Update a View /// You can use CREATE OR REPLACE: 
CREATE OR REPLACE VIEW high_earners AS
SELECT name, dept, salary FROM dep WHERE salary > 65000;

CREATE VIEW high_earners AS
SELECT name, dept, salary FROM dep WHERE salary > 60000;    # return error


#Drop a View 

DROP VIEW IF EXISTS high_earners; 

DROP VIEW high_earners; 



# CTE

# FIND AVG SALARY OF EACH DEPT  USEING CTE

 with dept_avg as(
 select dept , avg(salary) as "avg salary"
 from dep group by dept)
 select * from dept_avg;
 


# using cte  list employe whose salary is greater than the avg salaary of thire department 

with avg_salary as (
select dept , avg(salary) as "salary"
from dep group by dept
)
select e.* , a.salary as "avg" from dep e
join avg_salary a on e.dept = a.dept
where e.salary > a.salary; 

# write cte to rank employes within each department  by salary (higest to lowest ) 

with ranked_emp as (
 select * , rank() over(partition by dept order by salary desc ) as "rank" 
 from dep)
 select * from ranked_emp;
 
 # using cte , find the highest paid emply in the companay 
 
 with higest_salary as (
 select * from dep order by salary desc
 ) select * from higest_salary limit 1;
 
 with max_salary as (

 select max(salary) as top_salary from dep 
 )
 select d.* from dep d
 join max_salary m 
 on d.salary= m.top_salary;
 
 
 # create a recursive cte  that gernates no. from 1 - 5
 
 with recursive nums as (
 select 1 as n 
 union all
 select n +1 from nums where n < 5)
 select * from nums;
 
 
 create table empe (
 emp_id int , 
 name varchar(20) , 
 dept varchar(20) , 
 salary  int , 
 managr_id int );

insert into empe ( emp_id , name  , dept , salary ) values
(1, "alice"  , "hr" , 50000  ) ;

insert into empe ( emp_id , name  , dept , salary , managr_id) values
(2, "bob"  , "hr" , 50000 , 1 ) , 
(3, "charlie"  , "hr" , 50000 , 1 ) , 
(4, "david"  , "hr" , 50000 , 2 ) , 
(5, "eve"  , "hr" , 50000 , 2 ) , 
(6, "frank"  , "hr" , 50000 , 5 ) ; 
 
 
 
#Use a CTE to calculate the average salary for each department.
WITH dept_avg AS (
    SELECT department, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department
)
SELECT * FROM dept_avg;

#Find employees who earn more than the average salary of their department.
WITH dept_avg AS (
    SELECT department, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department
)
SELECT e.*
FROM employees e
JOIN dept_avg d ON e.department = d.department
WHERE e.salary > d.avg_salary;

#List employees and their rank within their department based on salary.
WITH ranked_emps AS (
    SELECT *,
           RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank
    FROM employees
)
SELECT * FROM ranked_emps; 
#Identify the highest-paid employee using a CTE.
WITH max_sal AS (
    SELECT MAX(salary) AS highest_salary FROM employees
)
SELECT e.*
FROM employees e
JOIN max_sal m ON e.salary = m.highest_salary; 

#Find employees who are managers (i.e., whose emp_id is referred to in manager_id).
WITH managers AS (
    SELECT DISTINCT manager_id
    FROM employees
    WHERE manager_id IS NOT NULL
)
SELECT e.*
FROM employees e
JOIN managers m ON e.emp_id = m.manager_id; 

#Use a CTE to show each employee along with their manager's name.
WITH manager_names AS (
    SELECT emp_id AS manager_id, name AS manager_name
    FROM employees
)
SELECT e.name AS employee_name, m.manager_name
FROM employees e
LEFT JOIN manager_names m ON e.manager_id = m.manager_id; 

#Use a CTE to count the number of employees in each department.
WITH dept_counts AS (
    SELECT department, COUNT(*) AS num_employees
    FROM employees
    GROUP BY department
)
SELECT * FROM dept_counts;
