-- grant access to TABLES
DECLARE
    v_tab VARCHAR2(255);
    v_strSQL CLOB;       
    v_user VARCHAR2(20) := 'testuser';
    CURSOR CurTab IS
        SELECT tab_name FROM TEMP_TABLES ORDER BY TAB_NAME;
BEGIN
    OPEN CurTab ;
    LOOP
        FETCH CurTab INTO v_tab;
        EXIT WHEN CurTab%NOTFOUND;
        BEGIN
            v_strSQL := 'grant SELECT, UPDATE, INSERT, DELETE on ' || v_tab || ' to ' || v_user  ; 
            DBMS_OUTPUT.PUT_LINE('Granting access to table v_strSQL: ' || v_strSQL);
            EXECUTE IMMEDIATE v_strSQL;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error granting access to table ' || v_tab || ': ' || SQLERRM);
        END;
    END LOOP;    
END;
/