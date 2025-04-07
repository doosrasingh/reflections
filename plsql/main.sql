DROP TABLE temp_columns ;
DROP TABLE temp_tables ;
commit;

CREATE TABLE temp_tables (
    tab_name VARCHAR2(255)
);

CREATE TABLE temp_columns (
    tablename VARCHAR2(255),
    columnname VARCHAR2(255),
    iskey NUMBER(1),
    columntype VARCHAR2(50)
);


-- fill temp_tables
INSERT ALL
    INTO TEMP_TABLES (TAB_NAME) VALUES ('Vertrieb_K')
    INTO TEMP_TABLES (TAB_NAME) VALUES ('Vertrieb_P')
    --INTO TEMP_TABLES (TAB_NAME) VALUES ('Dummy')
SELECT * FROM dual;
COMMIT;

-- fill temp_columns
INSERT ALL
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_K', 'VBELN', 1, 'VARCHAR2(10)')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_K', 'KTEXT', 0, 'VARCHAR2(40)')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_K', 'ABSTK', 0, 'VARCHAR2(1)')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_K', 'BSTKD', 0, 'VARCHAR2(35)')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_K', 'VUNTDAT', 0, 'DATE')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_K', 'VKUESCH', 0, 'VARCHAR2(4)')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_K', 'VWUNDAT', 0, 'DATE')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_K', 'VENDDAT', 0, 'DATE')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_K', 'VBEDKUE', 0, 'DATE')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_K', 'VBEGDAT', 0, 'DATE')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_P', 'VBELN', 1, 'VARCHAR2(10)')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_P', 'POSNR', 1, 'VARCHAR2(6)')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_P', 'BSTKD', 0, 'VARCHAR2(35)')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_P', 'VUNTDAT', 0, 'DATE')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_P', 'VKUESCH', 0, 'VARCHAR2(4)')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_P', 'VWUNDAT', 0, 'DATE')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_P', 'VENDDAT', 0, 'DATE')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_P', 'VBEDKUE', 0, 'DATE')
    INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Vertrieb_P', 'VBEGDAT', 0, 'DATE')
    --INTO TEMP_COLUMNS (tablename, columnname, iskey, columntype) VALUES ('Dummy', 'TESTCOL', 0, 'VARCHAR(2)')
SELECT * FROM dual;
COMMIT;


-- drop existing views
DECLARE
    v_tab VARCHAR2(255);
    v_strSQL CLOB;
    v_viewprefix VARCHAR2(30) := 'v_';
    CURSOR CurTab IS
        SELECT tab_name FROM TEMP_TABLES ORDER BY TAB_NAME;
BEGIN
    OPEN CurTab;
    LOOP
        FETCH CurTab INTO v_tab;
        EXIT when CurTab%NOTFOUND;
        BEGIN
            v_strSQL := 'DROP VIEW ' || v_viewprefix || v_tab;
            DBMS_OUTPUT.PUT_LINE('view-v_strSQL ' || v_strSQL);
            EXECUTE IMMEDIATE v_strSQL;
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE = -942 THEN
                    NULL;
                ELSE 
                    DBMS_OUTPUT.PUT_LINE('Error dropping view ' || v_viewprefix || v_tab || ': ' || SQLERRM);
                END IF;
        END;
    END LOOP;
    CLOSE CurTab;
END;
/

-- drop existing tables
DECLARE
    v_tab VARCHAR2(255);
    v_strSQL CLOB;    
    CURSOR CurTab IS
        SELECT tab_name FROM TEMP_TABLES ORDER BY TAB_NAME;
BEGIN
    OPEN CurTab;
    LOOP
        FETCH CurTab INTO v_tab;
        EXIT when CurTab%NOTFOUND;
        BEGIN
            v_strSQL := 'DROP TABLE ' || v_tab;
            DBMS_OUTPUT.PUT_LINE('table-v_strSQL ' || v_strSQL);
            EXECUTE IMMEDIATE v_strSQL;
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE = -942 THEN
                    NULL;
                ELSE 
                    DBMS_OUTPUT.PUT_LINE('Error dropping table ' || v_tab || ': ' || SQLERRM);
                END IF;
        END;
    END LOOP;
    CLOSE CurTab;
END;
/

-- create TABLES incl COLUMNS
DECLARE
    v_tab VARCHAR2(255);
    v_strSQL CLOB;
    v_fieldslist CLOB;
    CURSOR CurTab IS
        SELECT tab_name FROM TEMP_TABLES ORDER BY TAB_NAME;
BEGIN
    OPEN CurTab;
    LOOP
        FETCH CurTab INTO v_tab;
        EXIT WHEN CurTab%NOTFOUND;
        BEGIN
            v_strSQL := 'CREATE TABLE ' || v_tab || ' ';
            --v_fieldslist := NULL;
            SELECT LISTAGG(COLUMNNAME || ' ' || COLUMNTYPE || case when ISKEY=1 then ' NOT NULL' END, ', ' ) 
            INTO v_fieldslist
            FROM TEMP_COLUMNS 
            WHERE tablename = v_tab;
            v_strSQL := v_strSQL || '(' || v_fieldslist || ')';            
            DBMS_OUTPUT.PUT_LINE('Creating Table v_strSQL: ' || v_strSQL);
            EXECUTE IMMEDIATE v_strSQL;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error creating table ' || v_tab || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/

-- create INDEXES for the tables with PRIMARY KEY
DECLARE
    v_tab VARCHAR2(255);
    v_strSQL CLOB;
    v_fieldslist CLOB;
    v_columnname VARCHAR2(255);
    v_indexprefix VARCHAR2(20) := 'IDX_';
    CURSOR CurTab IS
        SELECT tab_name FROM TEMP_TABLES ORDER BY TAB_NAME;
BEGIN
    OPEN CurTab ;
    LOOP
        FETCH CurTab INTO v_tab;
        EXIT WHEN CurTab%NOTFOUND;
        BEGIN
            -- get PRIMARY KEY columns for index
            v_fieldslist := NULL;

            SELECT listagg(COLUMNNAME, ', ') INTO v_fieldslist
            FROM TEMP_COLUMNS 
            WHERE TABLENAME = v_tab AND iskey = 1;

            IF v_fieldslist IS NOT NULL THEN
                v_strSQL := 'CREATE INDEX ' || v_indexprefix || v_tab || ' ON ' || v_tab || ' (' || v_fieldslist || ')';
                DBMS_OUTPUT.PUT_LINE('Creating Index v_strSQL: ' || v_strSQL);
                EXECUTE IMMEDIATE v_strSQL;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error creating index ' || v_tab || ': ' || SQLERRM);
        END;
    END LOOP;    
END;
/

-- create VIEWS
DECLARE
    v_tab VARCHAR2(255);
    v_strSQL CLOB;
    v_fieldslist CLOB;
    v_viewprefix VARCHAR2(20) := 'v_';
    CURSOR CurTab IS
        SELECT tab_name FROM TEMP_TABLES ORDER BY TAB_NAME;
BEGIN
    OPEN CurTab ;
    LOOP
        FETCH CurTab INTO v_tab;
        EXIT WHEN CurTab%NOTFOUND;
        BEGIN
            -- get PRIMARY KEY columns for index
            v_fieldslist := NULL;

            SELECT listagg(COLUMNNAME, ', ') INTO v_fieldslist
            FROM TEMP_COLUMNS 
            WHERE TABLENAME = v_tab;

            IF v_fieldslist IS NOT NULL THEN
                v_strSQL := 'CREATE OR REPLACE VIEW ' || v_viewprefix || v_tab || ' AS SELECT ' || v_fieldslist || ' FROM ' || v_tab;
                DBMS_OUTPUT.PUT_LINE('Creating view v_strSQL: ' || v_strSQL);
                EXECUTE IMMEDIATE v_strSQL;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error creating index ' || v_tab || ': ' || SQLERRM);
        END;
    END LOOP;    
END;
/

-- create INSTEAD OF insert triggers
DECLARE
    CURSOR table_cursor IS
        select tab_name FROM temp_tables;
    CURSOR column_cursor (p_table_name NVARCHAR2) IS
        select columnname, iskey, columntype from temp_columns WHERE tablename = p_table_name;
    v_trigger_sql CLOB;
    v_viewprefix VARCHAR2(20) := 'v_';
    v_triggerprefix VARCHAR2(20) := 'trg_insert_';
    v_multiple NUMBER := 0;
    v_count_non_primkey_fields NUMBER := 0;
BEGIN
    FOR table_rec in table_cursor LOOP
        -- count the number of non-key fields in the table
        SELECT COUNT(*) INTO v_count_non_primkey_fields FROM temp_columns WHERE tablename = table_rec.tab_name AND iskey = 0;

        -- create sql for INSTEAD OF trigger - assumption at least one primary key, there could be 0 or many non-key fields
        v_trigger_sql := 'CREATE OR REPLACE TRIGGER ' || v_triggerprefix || v_viewprefix || table_rec.tab_name || ' ' ||
                         'INSTEAD OF INSERT ON ' || v_viewprefix || table_rec.tab_name || ' ' ||
                         'FOR EACH ROW ' ||
                         'DECLARE v_count NUMBER := 0; BEGIN ' ||
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
        v_trigger_sql := v_trigger_sql || ';';
        
        -- UPDATE SECTION
        -- if table has non-primary key fields, generate UPDATE, we assume that atleast one Primary Key field exists
        IF v_count_non_primkey_fields > 0 THEN
            v_trigger_sql := v_trigger_sql || 'IF v_count > 0 THEN UPDATE ' || table_rec.tab_name || ' SET ';
            -- collect UPDATE columns
            FOR column_rec in column_cursor(table_rec.tab_name) LOOP
                IF column_rec.iskey = 0 THEN
                    v_trigger_sql := v_trigger_sql || column_rec.columnname || ' = :NEW.' || column_rec.columnname || ', ';
                END IF;
            END LOOP;
            -- remove final comma, add WHERE
            v_trigger_sql :=  RTRIM(v_trigger_sql, ', ');
            v_trigger_sql := v_trigger_sql || ' WHERE ';
            -- create WHERE clause with Primary Keys
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
            -- add ELSE part which will be the insert section
            v_trigger_sql := v_trigger_sql || '; ELSE ';
        END IF;
        -- UPDATE SECTION ends

        -- INSERT SECTION starts
        v_trigger_sql := v_trigger_sql || 'INSERT INTO ' || table_rec.tab_name || ' (';
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
        v_trigger_sql := v_trigger_sql || '); ';
        -- INSERT SECTION ends

        -- close the first BEGIN or close the IF from the UPDATE section and then close BEGIN
        IF v_count_non_primkey_fields > 0 THEN
            v_trigger_sql := v_trigger_sql || 'END IF; END;';
        ELSE
            v_trigger_sql := v_trigger_sql || 'END;';
        END IF;

        DBMS_OUTPUT.PUT_LINE('Creating trigger v_trigger_sql: ' || v_trigger_sql);
        EXECUTE IMMEDIATE v_trigger_sql;
    END LOOP;
END;
/

