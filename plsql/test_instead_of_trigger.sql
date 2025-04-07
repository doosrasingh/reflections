CREATE TABLE departments (
    dept_id int PRIMARY KEY,
    dept_name VARCHAR2(30)
);

CREATE TABLE employees (
    emp_id int PRIMARY KEY,
    emp_name VARCHAR2(100),
    dept_id INT
);

insert into departments values ( 1, 'New dept' );
insert into employees values ( 1, 'Arun', 1);
commit;

create or replace view department_employees as 
  select * from departments d
  join   employees e
  using  ( dept_id );
  
select * from department_employees;

insert into department_employees ( dept_id, dept_name, emp_id ) 
values ( 2, 'Second dept', 2 );

create or replace trigger deem_instead_of_ins_t
instead of insert on department_employees
for each row
begin
  merge into departments 
  using dual 
  on   ( dept_id = :new.dept_id )
  when not matched then
    insert values ( :new.dept_id, :new.dept_name )
  when matched then
    update set dept_name = :new.dept_name;
    
  insert into employees values ( :new.emp_id, :new.emp_name, :new.dept_id ); 
end;
/


insert into department_employees ( dept_id, dept_name, emp_id, emp_name ) 
values ( 2, 'Second dept', 2, 'Ajit');
commit;
insert into department_employees ( department_id, department_name, employee_id ) 
values ( 1, 'Changed name', 3 );

select * from department_employees order by dept_id;

/*
CREATE OR REPLACE TRIGGER view_instead_of_insert
INSTEAD OF INSERT ON your_view_name
FOR EACH ROW
BEGIN
  -- Check if the primary key already exists in the table
  DECLARE
    pk_exists NUMBER;
  BEGIN
    SELECT COUNT(*) INTO pk_exists
    FROM your_table_name
    WHERE primary_key_column = :NEW.primary_key_column;

    IF pk_exists > 0 THEN
      -- Update the existing row
      UPDATE your_table_name
      SET column1 = :NEW.column1,
          column2 = :NEW.column2
      WHERE primary_key_column = :NEW.primary_key_column;
    ELSE
      -- Insert a new row
      INSERT INTO your_table_name (primary_key_column, column1, column2)
      VALUES (:NEW.primary_key_column, :NEW.column1, :NEW.column2);
    END IF;
  END;
END;



MERGE INTO your_table_name target
USING (SELECT :NEW.primary_key_column AS primary_key_column,
              :NEW.column1 AS column1,
              :NEW.column2 AS column2
       FROM dual) source
ON (target.primary_key_column = source.primary_key_column)
WHEN MATCHED THEN
  UPDATE SET target.column1 = source.column1,
             target.column2 = source.column2
WHEN NOT MATCHED THEN
  INSERT (primary_key_column, column1, column2)
  VALUES (source.primary_key_column, source.column1, source.column2);

*/

