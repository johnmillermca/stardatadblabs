SET FEEDBACK OFF SERVEROUTPUT ON SIZE UNLIMITED
DECLARE
  TYPE t_str IS TABLE OF VARCHAR2(60) INDEX BY PLS_INTEGER;
  v_cats  t_str;
  v_subs  t_str;
  v_id    NUMBER;
BEGIN
  v_cats(1):='Electronics'; v_cats(2):='Clothing'; v_cats(3):='Food';
  v_cats(4):='Books';       v_cats(5):='Sports';   v_cats(6):='Home';
  v_cats(7):='Toys';        v_cats(8):='Automotive';v_cats(9):='Health';
  v_cats(10):='Garden';
  v_subs(1):='Premium'; v_subs(2):='Basic';   v_subs(3):='Pro';
  v_subs(4):='Lite';    v_subs(5):='Ultra';   v_subs(6):='Classic';
  v_subs(7):='Smart';   v_subs(8):='Eco';     v_subs(9):='Deluxe';
  v_subs(10):='Value';
  FOR b IN 0..49 LOOP
    FOR i IN 1..10000 LOOP
      v_id := b*10000+i;
      INSERT INTO products(product_id,sku,product_name,category,subcategory,
        unit_price,cost_price,stock_qty,weight_kg,is_active,created_at,updated_at)
      VALUES(v_id,
        'SKU-'||LPAD(v_id,8,'0'),
        v_subs(MOD(v_id-1,10)+1)||' '||v_cats(MOD(v_id-1,10)+1)||' '||TO_CHAR(v_id),
        v_cats(MOD(v_id-1,10)+1),
        v_subs(MOD(v_id-1,5)+1),
        ROUND(DBMS_RANDOM.VALUE(0.99,9999.99),2),
        ROUND(DBMS_RANDOM.VALUE(0.50,5000),2),
        TRUNC(DBMS_RANDOM.VALUE(0,10000)),
        ROUND(DBMS_RANDOM.VALUE(0.01,50),3),
        CASE WHEN MOD(v_id,20)=0 THEN 'N' ELSE 'Y' END,
        SYSTIMESTAMP-NUMTODSINTERVAL(DBMS_RANDOM.VALUE(0,730),'DAY'),
        SYSTIMESTAMP);
    END LOOP;
    COMMIT;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('PRODUCTS loaded: 500000');
END;
/
EXIT;
