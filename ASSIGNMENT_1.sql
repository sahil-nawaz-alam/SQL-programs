			--- SUNEET_PAUL_SINGH ---
            
---	QUESTION_1 ---
		/*Create a database named company */
---	answer_1 ---
create database company ;

use company;

		/*Create a table named employees */
create table employees1(
id int  Primary Key  Auto_Increment,
name VARCHAR(100),
position VARCHAR(100),
salary DECIMAL(10,2),
date_of_joining DATE
);

---	QUESTION_2 ---	
		/*Insert data into the employees table:*/

---	answer_2 ---
		/*name= John Doe, position= Manager, salary=  55000.00, date_of_joining= 2020-01-15 */
insert into employees1(  name , position, salary , date_of_joining) 
values ( "John Doe"	, "Manager"		, 55000.00," 2020-01-15");

		/*name= Jane Smith, position= Developer, salary=  48000.00, date_of_joining=  2019-07-10 */
insert into employees1( name , position, salary , date_of_joining) 
values ("Jane Smith"		, "Developer"	, 48000.00, "2019-07-10");

		/*name=Alice Johnson,, position= Designer, salary=  45000.00, date_of_joining=  2021-03-22*/
insert into employees1( name , position, salary , date_of_joining) 
values ("Alice Johnson"	, "Designer"	, 45000.00, "2021-03-22");

		/* name= Bob Brown position= Developer, salary=  50000.00, date_of_joining=  2018-11-01 */
insert into employees1( name , position, salary , date_of_joining) 
values("Bob Brown"		, "Developer"	, 50000.00, "2018-11-01");

		/*check all data inserted*/
select * from employees1;

---	QUESTION_3 ---
		/*Write a query to retrieve all employees who are Developers.*/
---	answer_3 ---
select * from employees1 where position ="Developer";


set sql_safe_updates=0;

---	QUESTION_4 ---
		/*Write a query to update the salary of Alice Johnson to 46000.00.*/
---	answer_4 ---
update employees1
set salary=46000.00 where name ="Alice Johnson";

---	QUESTION_5 ---
		/*Write a query to delete the employee record for Bob Brown.*/
---	answer_5 ---
delete from employees where name = "Bob Brown";

---	QUESTION_6 ---
		/*Write a query to find the employees who have a salary greater than 48000.*/
---	answer_6 ---
select * from employees1 where salary < 48000;

---	QUESTION_7 ---
		/*Write a query to add a new column email to the employees table.*/
---	answer_7 ---
Alter table employees1 add email varchar(100);

---	QUESTION_8 ---
		/*Write a query to update the email for John Doe to john.doe@company.com.*/
---	answer_8 ---
update employees1
set email =" john.doe@company.com" where name ="John Doe";

---	QUESTION_9 ---
		/*Write a query to retrieve only the name and salary of all employees.*/
---	answer_9 ---
select name ,salary from employees1;

---	QUESTION_10 ---
		/*Write a query to count the number of employees who joined after January 1, 2020.*/
---	answer_10 ---
select count(*) from employees1 where date_of_joining > "2020-01-01";

---	QUESTION_11 ---
		/*Write a query to order the employees by salary in descending order.*/
---	answer_11 ---
select * from employees1 order by salary desc;

---	QUESTION_12 ---
		/*Write a query to drop the email column from the employees table.*/
---	answer_12 ---
ALTER TABLE employees1 DROP email;

---	QUESTION_13 ---
		/*Write a query to find the employee with the highest salary.*/
---	answer_13 ---
select * from employees order by salary desc  limit 1;




