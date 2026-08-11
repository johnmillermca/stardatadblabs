SET FEEDBACK OFF SERVEROUTPUT ON SIZE UNLIMITED
DECLARE
  TYPE t_str IS TABLE OF VARCHAR2(30) INDEX BY PLS_INTEGER;
  v_statuses t_str;
  v_methods  t_str;
  v_id       NUMBER;
  v_cid      NUMBER;
  v_days     NUMBER;
  v_stat     VARCHAR2(20);
  v_odate    TIMESTAMP;
BEGIN
  v_statuses(1):='PENDING';   v_statuses(2):='CONFIRMED'; v_statuses(3):='SHIPPED';
  v_statuses(4):='DELIVERED'; v_statuses(5):='CANCELLED'; v_statuses(6):='RETURNED';
  v_methods(1):='CARD'; v_methods(2):='PAYPAL'; v_methods(3):='BANK_TRANSFER';
  v_methods(4):='CRYPTO'; v_methods(5):='VOUCHER';
  FOR b IN 0..299 LOOP
    FOR i IN 1..10000 LOOP
      v_id    := b*10000+i;
      v_cid   := TRUNC(DBMS_RANDOM.VALUE(1,2000001));
      v_days  := DBMS_RANDOM.VALUE(0,1095);
      v_stat  := v_statuses(TRUNC(DBMS_RANDOM.VALUE(1,7)));
      v_odate := SYSTIMESTAMP - NUMTODSINTERVAL(v_days,'DAY');
      INSERT INTO orders(order_id,customer_id,status,total_amount,discount_amount,
        tax_amount,shipping_city,shipping_country,payment_method,
        order_date,shipped_date,delivered_date,updated_at)
      VALUES(v_id, v_cid, v_stat,
        ROUND(DBMS_RANDOM.VALUE(5,5000),2),
        ROUND(DBMS_RANDOM.VALUE(0,200),2),
        ROUND(DBMS_RANDOM.VALUE(0,500),2),
        'City'||TO_CHAR(MOD(v_id,100)),
        'US',
        v_methods(TRUNC(DBMS_RANDOM.VALUE(1,6))),
        v_odate,
        CASE WHEN v_stat IN('SHIPPED','DELIVERED','RETURNED')
             THEN v_odate+NUMTODSINTERVAL(DBMS_RANDOM.VALUE(1,5),'DAY') ELSE NULL END,
        CASE WHEN v_stat='DELIVERED'
             THEN v_odate+NUMTODSINTERVAL(DBMS_RANDOM.VALUE(3,10),'DAY') ELSE NULL END,
        SYSTIMESTAMP);
    END LOOP;
    COMMIT;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('ORDERS loaded: 3000000');
END;
/
EXIT;
