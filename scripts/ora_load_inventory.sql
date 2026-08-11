SET FEEDBACK OFF SERVEROUTPUT ON SIZE UNLIMITED
DECLARE
  TYPE t_str IS TABLE OF VARCHAR2(20) INDEX BY PLS_INTEGER;
  v_types t_str;
  v_id    NUMBER;
  v_type  VARCHAR2(20);
BEGIN
  v_types(1):='RECEIPT'; v_types(2):='SALE'; v_types(3):='ADJUSTMENT';
  v_types(4):='RETURN';  v_types(5):='TRANSFER'; v_types(6):='WRITE_OFF';
  FOR b IN 0..299 LOOP
    FOR i IN 1..10000 LOOP
      v_id   := b*10000+i;
      v_type := v_types(TRUNC(DBMS_RANDOM.VALUE(1,7)));
      INSERT INTO inventory_events(event_id,product_id,event_type,delta_qty,
        warehouse_id,reference_id,notes,event_at)
      VALUES(v_id,
        TRUNC(DBMS_RANDOM.VALUE(1,500001)),
        v_type,
        CASE v_type
          WHEN 'SALE'      THEN -TRUNC(DBMS_RANDOM.VALUE(1,20))
          WHEN 'WRITE_OFF' THEN -TRUNC(DBMS_RANDOM.VALUE(1,100))
          ELSE TRUNC(DBMS_RANDOM.VALUE(1,500))
        END,
        TRUNC(DBMS_RANDOM.VALUE(1,51)),
        'REF-'||LPAD(TO_CHAR(v_id),10,'0'),
        'Batch '||TO_CHAR(b)||' load',
        SYSTIMESTAMP-NUMTODSINTERVAL(DBMS_RANDOM.VALUE(0,730),'DAY'));
    END LOOP;
    COMMIT;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('INVENTORY_EVENTS loaded: 3000000');
END;
/
EXIT;
