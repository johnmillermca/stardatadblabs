-- ============================================================
-- Star Knowledge Catalog — Doris Sample Data
-- Database: governance_demo
--
-- Inserts realistic-looking but entirely synthetic test data.
-- 20 customers, 40 orders, 40 payments, 10 products.
-- Apply after 001_create_schema.sql.
-- ============================================================

USE governance_demo;

-- ── Products ───────────────────────────────────────────────────────────────
INSERT INTO products (product_id, sku, name, category, subcategory, unit_price) VALUES
  (1,  'SKU-001', 'Wireless Noise-Cancelling Headphones', 'Electronics', 'Audio',     299.99),
  (2,  'SKU-002', 'Ergonomic Office Chair',               'Furniture',   'Seating',   499.00),
  (3,  'SKU-003', 'Stainless Steel Water Bottle 1L',      'Outdoors',    'Hydration',  34.99),
  (4,  'SKU-004', 'Mechanical Keyboard TKL',              'Electronics', 'Peripherals',149.99),
  (5,  'SKU-005', 'Yoga Mat Premium 6mm',                 'Sports',      'Yoga',       59.99),
  (6,  'SKU-006', 'Smart Watch Series X',                 'Electronics', 'Wearables', 399.00),
  (7,  'SKU-007', 'Cold Brew Coffee Maker',               'Kitchen',     'Beverages',  49.99),
  (8,  'SKU-008', 'Bamboo Cutting Board Set',             'Kitchen',     'Prep',       39.99),
  (9,  'SKU-009', 'Running Shoes Pro V2',                 'Sports',      'Footwear',  129.99),
  (10, 'SKU-010', '4K USB-C Monitor 27"',                 'Electronics', 'Displays',  649.99);

-- ── Customers ──────────────────────────────────────────────────────────────
-- PII data is realistic-looking but entirely synthetic (no real persons).
INSERT INTO customers
    (customer_id, full_name, email, phone_number, date_of_birth, national_id,
     street_address, city, country_code, ip_address, salary, customer_tier)
VALUES
  (1001,'Alice Fontaine',    'alice.fontaine@example.com',   '+1-415-555-0101','1988-03-14','SSN-001-00-0001','742 Evergreen Terrace',    'Springfield',  'US','192.168.1.101',78000.00,'gold'),
  (1002,'Bob Tremblay',      'bob.tremblay@example.com',     '+1-514-555-0202','1975-07-22','SSN-002-00-0002','1 Infinite Loop',          'Cupertino',    'US','10.0.0.55',    95000.00,'platinum'),
  (1003,'Clara Meier',       'clara.meier@example.de',       '+49-30-555-0303','1991-11-05','DE-NID-000003',  'Unter den Linden 77',      'Berlin',       'DE','172.16.5.200', 62000.00,'silver'),
  (1004,'David Osei',        'david.osei@example.gh',        '+233-20-555-0404','1983-01-30','GH-NID-000004', '12 Independence Ave',      'Accra',        'GH','10.10.1.9',    45000.00,'standard'),
  (1005,'Eva Novak',         'eva.novak@example.cz',         '+420-2-5550505', '1995-06-18','CZ-NID-000005', 'Wenceslas Square 4',       'Prague',       'CZ','192.0.2.15',   71000.00,'gold'),
  (1006,'Frank Okafor',      'frank.okafor@example.ng',      '+234-1-5550606', '1980-09-03','NG-NIN-000006', '3 Victoria Island Blvd',   'Lagos',        'NG','198.51.100.42', 58000.00,'silver'),
  (1007,'Grace Liu',         'grace.liu@example.cn',         '+86-10-5550707', '1998-12-27','CN-RIC-000007', '88 Renmin Road',           'Shanghai',     'CN','203.0.113.7',   89000.00,'platinum'),
  (1008,'Henry Johansson',   'henry.johansson@example.se',   '+46-8-5550808',  '1972-04-15','SE-NID-000008', 'Drottninggatan 55',        'Stockholm',    'SE','10.20.30.40',  112000.00,'platinum'),
  (1009,'Isla Petrov',       'isla.petrov@example.ru',       '+7-495-5550909', '1990-08-09','RU-SNILS-000009','Arbat Street 10',         'Moscow',       'RU','192.168.50.1',  67000.00,'gold'),
  (1010,'James Nakamura',    'james.nakamura@example.jp',    '+81-3-55501010', '1986-02-20','JP-NID-000010', '1-1 Roppongi Hills',       'Tokyo',        'JP','172.31.0.100', 105000.00,'platinum'),
  (1011,'Karen Schmidt',     'karen.schmidt@example.de',     '+49-89-55501111','1993-05-31','DE-NID-000011', 'Maximilianstrasse 12',     'Munich',       'DE','10.0.1.11',    73000.00,'gold'),
  (1012,'Leo Carvalho',      'leo.carvalho@example.br',      '+55-11-55501212','1978-10-08','BR-CPF-000012', 'Av Paulista 1374',         'São Paulo',    'BR','192.168.2.12',  54000.00,'silver'),
  (1013,'Mia Andersson',     'mia.andersson@example.se',     '+46-31-55501313','1997-07-04','SE-NID-000013', 'Avenyn 22',                'Gothenburg',   'SE','10.50.0.13',    81000.00,'gold'),
  (1014,'Noah Becker',       'noah.becker@example.ch',       '+41-44-55501414','1984-03-19','CH-AHV-000014', 'Bahnhofstrasse 100',       'Zurich',       'CH','172.20.10.14', 140000.00,'platinum'),
  (1015,'Olivia Brown',      'olivia.brown@example.com',     '+1-312-55501515','1992-11-23','SSN-015-00-0015','456 W Madison St',        'Chicago',      'US','192.168.3.15',  69000.00,'gold'),
  (1016,'Pierre Dupont',     'pierre.dupont@example.fr',     '+33-1-55501616', '1969-06-07','FR-NID-000016', '15 Rue de Rivoli',         'Paris',        'FR','10.100.0.16',   96000.00,'platinum'),
  (1017,'Quinn Walsh',       'quinn.walsh@example.ie',       '+353-1-55501717','2000-01-12','IE-PPS-000017', 'O''Connell Street 7',      'Dublin',       'IE','192.0.2.17',    48000.00,'standard'),
  (1018,'Rosa Fernandez',    'rosa.fernandez@example.es',    '+34-91-55501818','1987-09-25','ES-NID-000018', 'Gran Via 42',              'Madrid',       'ES','172.16.18.18',  77000.00,'gold'),
  (1019,'Sam Goldstein',     'sam.goldstein@example.com',    '+1-212-55501919','1982-04-11','SSN-019-00-0019','350 Fifth Avenue',        'New York',     'US','10.0.0.19',    125000.00,'platinum'),
  (1020,'Tina Hoffmann',     'tina.hoffmann@example.at',     '+43-1-55502020', '1996-08-17','AT-SV-000020',  'Kärntner Ring 5',          'Vienna',       'AT','192.168.4.20',  64000.00,'silver');

-- ── Orders ─────────────────────────────────────────────────────────────────
INSERT INTO orders (order_id, customer_id, order_date, status, total_amount, currency, channel) VALUES
  (5001,1001,'2024-01-15 09:23:00','delivered', 299.99,'USD','web'),
  (5002,1002,'2024-01-16 14:10:00','delivered', 649.99,'USD','mobile'),
  (5003,1001,'2024-02-03 11:45:00','delivered', 149.99,'USD','web'),
  (5004,1003,'2024-02-07 16:30:00','shipped',   499.00,'EUR','web'),
  (5005,1004,'2024-02-12 08:00:00','delivered',  34.99,'USD','web'),
  (5006,1005,'2024-02-20 19:55:00','delivered',  59.99,'CZK','mobile'),
  (5007,1006,'2024-03-01 10:10:00','delivered', 129.99,'USD','web'),
  (5008,1007,'2024-03-05 13:22:00','confirmed', 399.00,'CNY','app'),
  (5009,1008,'2024-03-08 07:15:00','delivered', 299.99,'SEK','web'),
  (5010,1009,'2024-03-11 22:00:00','delivered', 649.99,'RUB','mobile'),
  (5011,1010,'2024-03-15 09:30:00','delivered', 499.00,'JPY','web'),
  (5012,1011,'2024-03-20 16:00:00','shipped',   149.99,'EUR','web'),
  (5013,1012,'2024-03-25 11:10:00','delivered',  39.99,'BRL','mobile'),
  (5014,1013,'2024-04-01 14:55:00','delivered', 399.00,'SEK','app'),
  (5015,1014,'2024-04-05 08:45:00','delivered', 649.99,'CHF','web'),
  (5016,1015,'2024-04-10 17:30:00','confirmed', 299.99,'USD','web'),
  (5017,1016,'2024-04-12 12:00:00','delivered', 499.00,'EUR','mobile'),
  (5018,1017,'2024-04-18 09:00:00','pending',    34.99,'EUR','web'),
  (5019,1018,'2024-04-22 15:15:00','delivered', 129.99,'EUR','web'),
  (5020,1019,'2024-04-25 18:40:00','delivered', 649.99,'USD','app'),
  (5021,1020,'2024-05-01 10:20:00','delivered',  59.99,'EUR','web'),
  (5022,1001,'2024-05-05 14:00:00','delivered', 399.00,'USD','web'),
  (5023,1002,'2024-05-10 11:30:00','delivered',  49.99,'USD','mobile'),
  (5024,1003,'2024-05-15 16:45:00','returned',  299.99,'EUR','web'),
  (5025,1004,'2024-05-20 09:10:00','delivered',  59.99,'USD','web'),
  (5026,1005,'2024-05-22 13:25:00','delivered', 149.99,'CZK','app'),
  (5027,1006,'2024-05-28 18:00:00','shipped',   499.00,'USD','web'),
  (5028,1007,'2024-06-01 10:55:00','delivered', 129.99,'CNY','mobile'),
  (5029,1008,'2024-06-05 07:30:00','delivered', 649.99,'SEK','web'),
  (5030,1009,'2024-06-10 20:10:00','confirmed',  39.99,'RUB','app'),
  (5031,1010,'2024-06-12 09:45:00','delivered', 299.99,'JPY','web'),
  (5032,1011,'2024-06-15 14:20:00','delivered', 399.00,'EUR','web'),
  (5033,1012,'2024-06-18 11:00:00','delivered',  34.99,'BRL','mobile'),
  (5034,1013,'2024-06-20 16:30:00','delivered',  49.99,'SEK','web'),
  (5035,1014,'2024-06-22 08:15:00','delivered', 499.00,'CHF','web'),
  (5036,1015,'2024-06-25 17:00:00','delivered', 149.99,'USD','app'),
  (5037,1016,'2024-06-28 12:30:00','shipped',    59.99,'EUR','web'),
  (5038,1017,'2024-07-01 09:20:00','pending',    39.99,'EUR','mobile'),
  (5039,1018,'2024-07-03 15:40:00','delivered', 649.99,'EUR','web'),
  (5040,1019,'2024-07-05 18:55:00','delivered', 399.00,'USD','app');

-- ── Payments ───────────────────────────────────────────────────────────────
-- card_number and credit_card_cvv are synthetic — not real card numbers.
INSERT INTO payments
    (payment_id, order_id, card_number, credit_card_cvv, card_type,
     payment_method, amount, currency, status, gateway, transaction_ref, paid_at)
VALUES
  (9001,5001,'4532015112830001','101','Visa',      'credit_card',299.99,'USD','settled','Stripe','TXN-001','2024-01-15 09:24:00'),
  (9002,5002,'5425233430109002','202','Mastercard','credit_card',649.99,'USD','settled','Stripe','TXN-002','2024-01-16 14:11:00'),
  (9003,5003,'4532015112830003','103','Visa',      'credit_card',149.99,'USD','settled','Stripe','TXN-003','2024-02-03 11:46:00'),
  (9004,5004,'4916338506082004','204','Visa',      'credit_card',499.00,'EUR','settled','Adyen', 'TXN-004','2024-02-07 16:31:00'),
  (9005,5005,'5425233430109005','205','Mastercard','credit_card', 34.99,'USD','settled','Stripe','TXN-005','2024-02-12 08:01:00'),
  (9006,5006,'4532015112830006','106','Visa',      'credit_card', 59.99,'CZK','settled','Adyen', 'TXN-006','2024-02-20 19:56:00'),
  (9007,5007,'4916338506082007','307','Visa',      'credit_card',129.99,'USD','settled','Stripe','TXN-007','2024-03-01 10:11:00'),
  (9008,5008,'5425233430109008','208','Mastercard','credit_card',399.00,'CNY','settled','Alipay','TXN-008','2024-03-05 13:23:00'),
  (9009,5009,'4532015112830009','109','Visa',      'credit_card',299.99,'SEK','settled','Klarna','TXN-009','2024-03-08 07:16:00'),
  (9010,5010,'4916338506082010','210','Visa',      'credit_card',649.99,'RUB','settled','Sberbank','TXN-010','2024-03-11 22:01:00'),
  (9011,5011,'5425233430109011','311','Mastercard','credit_card',499.00,'JPY','settled','GMO',   'TXN-011','2024-03-15 09:31:00'),
  (9012,5012,'4532015112830012','212','Visa',      'credit_card',149.99,'EUR','settled','Adyen', 'TXN-012','2024-03-20 16:01:00'),
  (9013,5013,'4916338506082013','413','Visa',      'credit_card', 39.99,'BRL','settled','PagSeguro','TXN-013','2024-03-25 11:11:00'),
  (9014,5014,'5425233430109014','214','Mastercard','credit_card',399.00,'SEK','settled','Klarna','TXN-014','2024-04-01 14:56:00'),
  (9015,5015,'4532015112830015','115','Visa',      'credit_card',649.99,'CHF','settled','PayPal','TXN-015','2024-04-05 08:46:00'),
  (9016,5016,'4916338506082016','316','Visa',      'credit_card',299.99,'USD','pending','Stripe','TXN-016',NULL),
  (9017,5017,'5425233430109017','217','Mastercard','credit_card',499.00,'EUR','settled','Adyen', 'TXN-017','2024-04-12 12:01:00'),
  (9018,5018,'4532015112830018','118','Visa',      'credit_card', 34.99,'EUR','pending','Stripe','TXN-018',NULL),
  (9019,5019,'4916338506082019','219','Visa',      'credit_card',129.99,'EUR','settled','Redsys','TXN-019','2024-04-22 15:16:00'),
  (9020,5020,'5425233430109020','420','Mastercard','credit_card',649.99,'USD','settled','Stripe','TXN-020','2024-04-25 18:41:00'),
  (9021,5021,'4532015112830021','121','Visa',      'credit_card', 59.99,'EUR','settled','Adyen', 'TXN-021','2024-05-01 10:21:00'),
  (9022,5022,'4916338506082022','222','Visa',      'credit_card',399.00,'USD','settled','Stripe','TXN-022','2024-05-05 14:01:00'),
  (9023,5023,'5425233430109023','323','Mastercard','credit_card', 49.99,'USD','settled','Stripe','TXN-023','2024-05-10 11:31:00'),
  (9024,5024,'4532015112830024','124','Visa',      'credit_card',299.99,'EUR','refunded','Adyen','TXN-024','2024-05-15 16:46:00'),
  (9025,5025,'4916338506082025','225','Visa',      'credit_card', 59.99,'USD','settled','Stripe','TXN-025','2024-05-20 09:11:00'),
  (9026,5026,'5425233430109026','426','Mastercard','credit_card',149.99,'CZK','settled','Adyen', 'TXN-026','2024-05-22 13:26:00'),
  (9027,5027,'4532015112830027','127','Visa',      'credit_card',499.00,'USD','pending','Stripe','TXN-027',NULL),
  (9028,5028,'4916338506082028','228','Visa',      'credit_card',129.99,'CNY','settled','Alipay','TXN-028','2024-06-01 10:56:00'),
  (9029,5029,'5425233430109029','329','Mastercard','credit_card',649.99,'SEK','settled','Klarna','TXN-029','2024-06-05 07:31:00'),
  (9030,5030,'4532015112830030','130','Visa',      'credit_card', 39.99,'RUB','pending','Sberbank','TXN-030',NULL),
  (9031,5031,'4916338506082031','231','Visa',      'credit_card',299.99,'JPY','settled','GMO',   'TXN-031','2024-06-12 09:46:00'),
  (9032,5032,'5425233430109032','432','Mastercard','credit_card',399.00,'EUR','settled','Adyen', 'TXN-032','2024-06-15 14:21:00'),
  (9033,5033,'4532015112830033','133','Visa',      'credit_card', 34.99,'BRL','settled','PagSeguro','TXN-033','2024-06-18 11:01:00'),
  (9034,5034,'4916338506082034','234','Visa',      'credit_card', 49.99,'SEK','settled','Klarna','TXN-034','2024-06-20 16:31:00'),
  (9035,5035,'5425233430109035','335','Mastercard','credit_card',499.00,'CHF','settled','PayPal','TXN-035','2024-06-22 08:16:00'),
  (9036,5036,'4532015112830036','136','Visa',      'credit_card',149.99,'USD','settled','Stripe','TXN-036','2024-06-25 17:01:00'),
  (9037,5037,'4916338506082037','237','Visa',      'credit_card', 59.99,'EUR','pending','Adyen', 'TXN-037',NULL),
  (9038,5038,'5425233430109038','438','Mastercard','credit_card', 39.99,'EUR','pending','Stripe','TXN-038',NULL),
  (9039,5039,'4532015112830039','139','Visa',      'credit_card',649.99,'EUR','settled','Adyen', 'TXN-039','2024-07-03 15:41:00'),
  (9040,5040,'4916338506082040','240','Visa',      'credit_card',399.00,'USD','settled','Stripe','TXN-040','2024-07-05 18:56:00');
