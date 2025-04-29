-- create INSTEAD OF insert triggers
DECLARE
    CURSOR table_cursor IS
        select tab_name FROM temp_tables;
    CURSOR column_cursor (p_table_name NVARCHAR2) IS
        select columnname, iskey, columntype from temp_columns WHERE tablename = p_table_name;
    v_trigger_sql CLOB;
    v_viewprefix VARCHAR2(20) := 'v_';
    v_triggerprefix VARCHAR2(20) := 'trg_Insert';
    v_multiple NUMBER := 0;
    v_count_non_primkey_fields NUMBER := 0;
BEGIN
    FOR table_rec in table_cursor LOOP
        -- count the number of non-key fields in the table
        SELECT COUNT(*) INTO v_count_non_primkey_fields FROM temp_columns WHERE tablename = table_rec.tab_name AND iskey = 0;
        
        -- create sql for INSTEAD OF trigger - assumption at least one primary key, there could be 0 or many non-key fields
        v_trigger_sql := 'CREATE OR REPLACE TRIGGER ' || v_triggerprefix || v_viewprefix || table_rec.tab_name || ' ' ||
                         'INSTEAD OF INSERT ON ' || v_viewprefix || table_rec.tab_name || ' ' ||
                         'FOR EACH ROW ';
        
        IF v_count_non_primkey_fields > 0 THEN
            -- create MERGE based trigger
            v_trigger_sql := v_trigger_sql || 'BEGIN MERGE INTO ' || table_rec.tab_name || ' USING DUAL ON (';

             -- check primary keys, get the primary key columns, we assume that atleast one Primary Key field exists
            v_multiple := 0;        
            FOR column_rec IN column_cursor(table_rec.tab_name) LOOP
                IF column_rec.iskey = 1 THEN
                    IF v_multiple = 0 THEN
                        v_trigger_sql := v_trigger_sql || column_rec.columnname || ' = :NEW.' || column_rec.columnname;
                        v_multiple := 1;
                    ELSE 
                        v_trigger_sql := v_trigger_sql || ' AND ' || column_rec.columnname || ' = :NEW.' || column_rec.columnname;
                    END IF;
                END IF;
            END LOOP;
            v_trigger_sql := v_trigger_sql ||  ')';

            -- we assume at least one non primary key field exists, collect UPDATE columns             
            v_multiple := 0; 
            v_trigger_sql := v_trigger_sql || ' WHEN MATCHED THEN UPDATE SET ';
            FOR column_rec in column_cursor(table_rec.tab_name) LOOP  
                IF column_rec.iskey = 0 THEN
                    IF v_multiple = 0 THEN
                        v_trigger_sql := v_trigger_sql || column_rec.columnname || ' = :NEW.' || column_rec.columnname;
                        v_multiple := 1;
                    ELSE 
                        v_trigger_sql := v_trigger_sql || ', ' || column_rec.columnname || ' = :NEW.' || column_rec.columnname;
                    END IF; 
                END IF;                 
            END LOOP;
                    
            -- collect column names for INSERT
            v_trigger_sql := v_trigger_sql || ' WHEN NOT MATCHED THEN INSERT (';            
            v_multiple := 0;
            FOR column_rec IN column_cursor(table_rec.tab_name) LOOP            
                IF v_multiple = 0 THEN
                    v_trigger_sql := v_trigger_sql || column_rec.columnname;
                    v_multiple := 1;
                ELSE 
                    v_trigger_sql := v_trigger_sql || ', ' || column_rec.columnname;
                END IF;            
            END LOOP;
            v_trigger_sql := v_trigger_sql || ') VALUES (' ;
            -- collect column values for INSERT
            v_multiple := 0;
            FOR column_rec IN column_cursor(table_rec.tab_name) LOOP            
                IF v_multiple = 0 THEN
                    v_trigger_sql := v_trigger_sql || ':NEW.' || column_rec.columnname;
                    v_multiple := 1;
                ELSE 
                    v_trigger_sql := v_trigger_sql || ', :NEW.' || column_rec.columnname;
                END IF;            
            END LOOP;
            v_trigger_sql := v_trigger_sql || '); END;';

        ELSE  
            -- create UPDATE/INSERT based trigger - only insert as there are no non-primary key
            v_trigger_sql := v_trigger_sql || 'DECLARE v_count NUMBER := 0; BEGIN ' ||
                                              'SELECT COUNT(*) INTO v_count FROM ' || v_viewprefix || table_rec.tab_name || ' WHERE ';

            -- check primary keys 
            v_multiple := 0;
            FOR column_rec IN column_cursor(table_rec.tab_name) LOOP
                IF column_rec.iskey = 1 THEN
                    IF v_multiple = 0 THEN
                        v_trigger_sql := v_trigger_sql || column_rec.columnname || ' = :NEW.' || column_rec.columnname;
                        v_multiple := 1;
                    ELSE 
                        v_trigger_sql := v_trigger_sql || ' AND ' || column_rec.columnname || ' = :NEW.' || column_rec.columnname;
                    END IF;
                END IF;
            END LOOP;
            v_trigger_sql := v_trigger_sql || '; ';

            -- INSERT SECTION starts
            v_trigger_sql := v_trigger_sql || 'IF v_count = 0 THEN INSERT INTO ' || table_rec.tab_name || ' (';
            -- collect column names for INSERT
            v_multiple := 0;
            FOR column_rec IN column_cursor(table_rec.tab_name) LOOP            
                IF v_multiple = 0 THEN
                    v_trigger_sql := v_trigger_sql || column_rec.columnname;
                    v_multiple := 1;
                ELSE 
                    v_trigger_sql := v_trigger_sql || ', ' || column_rec.columnname;
                END IF;            
            END LOOP;
            v_trigger_sql := v_trigger_sql || ') VALUES (' ;
            -- collect column values for INSERT
            v_multiple := 0;
            FOR column_rec IN column_cursor(table_rec.tab_name) LOOP            
                IF v_multiple = 0 THEN
                    v_trigger_sql := v_trigger_sql || ':NEW.' || column_rec.columnname;
                    v_multiple := 1;
                ELSE 
                    v_trigger_sql := v_trigger_sql || ', :NEW.' || column_rec.columnname;
                END IF;            
            END LOOP;
            v_trigger_sql := v_trigger_sql || '); END IF; ';
            -- INSERT SECTION ends

            -- close the first BEGIN or close the IF from the UPDATE section and then close BEGIN
            v_trigger_sql := v_trigger_sql || 'END;'; 
        
        END IF;

        DBMS_OUTPUT.PUT_LINE('Creating trigger v_trigger_sql: ' || v_trigger_sql);
        EXECUTE IMMEDIATE v_trigger_sql;

    END LOOP;
END;