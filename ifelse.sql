
create database ifelse;
use ifelse;

select * from emp;

#WRITE A QUERY TO CALCULATE A 10%BONUS IF THE SALARY IS GREATER THAN 
#OR EQUAL TO 60000 OUTHERWISE,CALCULATE  A 5% BONUS DISLPAY EMP_ID,NAME AND CALCULATED BONUS

 select emp_id , emp_name ,
 if(salary >=60000 , salary*0.1, salary*0.05) as bones from emp;
 
#WRITE A QUERY TO CLASSIFY EMPLOYEE AS EXPERIENCED IF THEIR JOINING 
#YEAR IS BEFORE 2020 AND NEWCOMES OTHERWISE DISPLAY EMP_ID,NAMME AND THE CATEGORY
 
 select emp_id , emp_name ,joining_year, if(joining_year >2020, "EXPerienced" , "NEWCOMER") as category from emp;
 
 
#WRITE A QUERY TO DISPLAY EMP_ID,NAME AND A STATUS OF HIGH EARNER OR AVERAGE EARNER 
#FOR EMPLOYEES IN THE IT DEPARTMENT ,DEPENDING ON WHETHER THEIR SALARY EXCEEDS 75000

 select emp_id , emp_name  , if(salary>75000 , "HIGH EARNER" , "AVG EARNER") AS STATUS from emp where dept ="IT";
 
 # WRITE A QUERY TO MARK EMP AS "ELIGIBLE" FOR PROMOTION  IF THEY ARE IN IT DEPT 
 #AND THEIR SALARY IS GREATER THAN 70000 MARK OTHER  as "NOT ELIGIABLE"
 
 select emp_name , dept , if(salary >75000 , "ELIGIBLE" , "NOT ELIGIBLE") AS PROMOTION FROM  emp where dept= "IT";
 
 # DETERMINE IF EMP ID IN THE EMPL TABLE IS EVEN OR ODD
 
  select emp_name , emp_id , if(emp_id%2 !=0 , "odd" , "even") AS evenod FROM  emp ;
  
  # CHECK WHETHER JOINING YEAR IS A LEAP YEAR OR NOT
  
  select emp_name ,joining_year, if(joining_year%4 = 0 , "leap" , "not leap") as year from emp;