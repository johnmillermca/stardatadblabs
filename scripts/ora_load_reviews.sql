SET FEEDBACK OFF SERVEROUTPUT ON SIZE UNLIMITED
DECLARE
  v_id    NUMBER;
  v_txt   VARCHAR2(4000);
BEGIN
  FOR b IN 0..199 LOOP
    FOR i IN 1..10000 LOOP
      v_id  := b*10000+i;
      v_txt := RPAD('Product review for item '||TO_CHAR(v_id)||'. Cache test data. ',
                    TRUNC(DBMS_RANDOM.VALUE(80,400)),
                    'Excellent quality, worth buying again. ');
      INSERT INTO product_reviews(review_id,product_id,customer_id,rating,
        title,review_text,helpful_cnt,created_at)
      VALUES(v_id,
        TRUNC(DBMS_RANDOM.VALUE(1,500001)),
        TRUNC(DBMS_RANDOM.VALUE(1,2000001)),
        TRUNC(DBMS_RANDOM.VALUE(1,6)),
        'Review title '||TO_CHAR(v_id),
        v_txt,
        TRUNC(DBMS_RANDOM.VALUE(0,1000)),
        SYSTIMESTAMP-NUMTODSINTERVAL(DBMS_RANDOM.VALUE(0,730),'DAY'));
    END LOOP;
    COMMIT;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('PRODUCT_REVIEWS loaded: 2000000');
END;
/
EXIT;
