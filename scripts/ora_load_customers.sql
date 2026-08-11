SET FEEDBACK OFF SERVEROUTPUT ON SIZE UNLIMITED
DECLARE
  TYPE t_str IS TABLE OF VARCHAR2(60) INDEX BY PLS_INTEGER;
  v_tiers   t_str;
  v_ctries  t_str;
  v_cities  t_str;
  v_id      NUMBER;
BEGIN
  v_tiers(1):='STANDARD'; v_tiers(2):='SILVER'; v_tiers(3):='GOLD'; v_tiers(4):='PLATINUM';
  v_ctries(1):='US'; v_ctries(2):='GB'; v_ctries(3):='CA'; v_ctries(4):='AU';
  v_ctries(5):='DE'; v_ctries(6):='FR'; v_ctries(7):='JP'; v_ctries(8):='IN';
  v_ctries(9):='BR'; v_ctries(10):='MX';
  v_cities(1):='New York';  v_cities(2):='London';     v_cities(3):='Toronto';
  v_cities(4):='Sydney';    v_cities(5):='Berlin';     v_cities(6):='Paris';
  v_cities(7):='Tokyo';     v_cities(8):='Mumbai';     v_cities(9):='Sao Paulo';
  v_cities(10):='Mexico City'; v_cities(11):='Chicago'; v_cities(12):='Manchester';
  v_cities(13):='Vancouver';v_cities(14):='Melbourne'; v_cities(15):='Frankfurt';
  v_cities(16):='Lyon';     v_cities(17):='Osaka';     v_cities(18):='Delhi';
  v_cities(19):='Rio';      v_cities(20):='Guadalajara';
  FOR b IN 0..199 LOOP
    FOR i IN 1..10000 LOOP
      v_id := b*10000+i;
      INSERT INTO customers(customer_id,first_name,last_name,email,phone,
        city,country_code,tier,credit_limit,is_active,created_at,updated_at)
      VALUES(v_id,
        'First'||TO_CHAR(v_id),
        'Last'||TO_CHAR(MOD(v_id,50000)),
        'user'||TO_CHAR(v_id)||'@ex'||TO_CHAR(MOD(v_id,100))||'.com',
        '+'||LPAD(TO_CHAR(TRUNC(DBMS_RANDOM.VALUE(1000000000,9999999999))),10,'0'),
        v_cities(MOD(v_id-1,20)+1),
        v_ctries(MOD(v_id-1,10)+1),
        v_tiers(TRUNC(DBMS_RANDOM.VALUE(1,5))),
        ROUND(DBMS_RANDOM.VALUE(500,50000),2),
        CASE WHEN MOD(v_id,50)=0 THEN 'N' ELSE 'Y' END,
        SYSTIMESTAMP-NUMTODSINTERVAL(DBMS_RANDOM.VALUE(0,1460),'DAY'),
        SYSTIMESTAMP);
    END LOOP;
    COMMIT;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('CUSTOMERS loaded: 2000000');
END;
/
EXIT;
