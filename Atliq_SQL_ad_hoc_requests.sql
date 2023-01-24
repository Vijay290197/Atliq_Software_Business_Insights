USE WAREHOUSE ATLIQ_SOFTWARES;
USE DATABASE ATLIQ_DATA;

-- DROP TABLE VJ_CUSTOMERS;
CREATE OR REPLACE TABLE VJ_CUSTOMERS (customer_code CHAR(8) PRIMARY KEY,
                                      custmer_name VARCHAR2(30),
                                      customer_type VARCHAR2(30)
                                      );
                                                  
-- DROP TABLE VJ_DATE;                                                  
CREATE OR REPLACE TABLE VJ_DATE (date DATE PRIMARY KEY,
                                 cy_date DATE,
                                 year INT,
                                 month_name CHAR (10),
                                 date_yy_mmm VARCHAR(15)
                                 );
-- DROP TABLE VJ_PRODUCTS;
CREATE OR REPLACE TABLE VJ_PRODUCTS (product_code CHAR(10) PRIMARY KEY,
                                     product_type VARCHAR(20)
                                     );
-- DROP TABLE VJ_MARKETS;                                    
CREATE OR REPLACE TABLE VJ_MARKETS (markets_code CHAR(10) PRIMARY KEY,
                                    markets_name VARCHAR(20),
                                    zone VARCHAR(20)
                                    );
                                    
-- DROP TABLE VJ_TRANSACTIONS;                                      
CREATE OR REPLACE TABLE VJ_TRANSACTIONS (product_code CHAR(10), -- FOREIGN KEY product_code REFERENCES VJ_PRODUCTS (product_code)
                                         customer_code CHAR(8), -- FOREIGN KEY customer_code REFERENCES VJ_CUSTOMERS(customer_code)
                                         market_code CHAR(10),  -- FOREIGN KEY market_code REFERENCES VJ_MARKETS (market_code)
                                         order_date DATE,       -- FOREIGN KEY order_date REFERENCES VJ_DATE (order_date)
                                         sales_qty INTEGER,     
                                         sales_amount INTEGER,
                                         currency CHAR(8),
                                         cost_price FLOAT
                                         );



SELECT * FROM VJ_CUSTOMERS; -- 38 ROWS
SELECT * FROM VJ_DATE; -- 1,126 ROWS
SELECT * FROM VJ_PRODUCTS; -- 279 ROWS
SELECT * FROM VJ_MARKETS; -- 17 ROWS
SELECT * FROM VJ_TRANSACTIONS; -- 1,48,395 ROWS


-- CREATE A MASTER TABLE;

CREATE OR REPLACE TABLE ATLIQ_MASTER_TABLE AS(
SELECT TXN.CUSTOMER_CODE, TXN.PRODUCT_CODE, TXN.MARKET_CODE,
       CUST.CUSTMER_NAME, CUST.CUSTOMER_TYPE, PROD.PRODUCT_TYPE,
       DT.DATE, DT.CY_DATE, DT.YEAR, DT.MONTH_NAME, DT.DATE_YY_MMM,
       MKT.MARKETS_NAME, MKT.ZONE,
       TXN.SALES_QTY, TXN.SALES_AMOUNT,TXN.CURRENCY, TXN.COST_PRICE
FROM VJ_TRANSACTIONS AS TXN
LEFT OUTER JOIN VJ_DATE AS DT ON TXN.order_date = DT.date
LEFT OUTER JOIN VJ_CUSTOMERS AS CUST ON TXN.customer_code = CUST.customer_code
LEFT OUTER JOIN VJ_PRODUCTS AS PROD ON TXN.product_code = PROD.product_code
LEFT OUTER JOIN VJ_MARKETS AS MKT ON TXN.market_code = MKT.markets_code);

SELECT *
FROM ATLIQ_MASTER_TABLE; -- 1,48,395 ROWS;

SELECT GET_DDL('TABLE', 'ATLIQ_MASTER_TABLE');

-- Show all customer records

SELECT COUNT(*)
FROM ATLIQ_MASTER_TABLE; --

-- DATA ANALYSIS USING SQL;

-- Checking Negative Sales or Not
SELECT *
FROM ATLIQ_MASTER_TABLE
WHERE SALES_AMOUNT < 0 ; --0 ROWS -- Note: It may be the loss of revenue whereby product returns and rebates exceed actual product sales.

-- Compute Profit Margin i.e.,
-- Profit = Sales Amount - COGS, after that --> Profit Margin = (Profit / Sales Amount) * 100

ALTER TABLE ATLIQ_MASTER_TABLE
ADD COLUMN PROFIT FLOAT, PROFIT_MARGIN FLOAT;

UPDATE ATLIQ_MASTER_TABLE
SET PROFIT = SALES_AMOUNT - COST_PRICE;

UPDATE ATLIQ_MASTER_TABLE
SET PROFIT_MARGIN = (PROFIT/SALES_AMOUNT)*100;

SELECT SALES_AMOUNT, COST_PRICE, PROFIT, PROFIT_MARGIN
FROM ATLIQ_MASTER_TABLE;

-- Checking Positive & Negative Profit Margin

SELECT SUM(PROFIT_MARGIN) -- -1241440
FROM ATLIQ_MASTER_TABLE
WHERE PROFIT_MARGIN < 0; -- 68503 Rows

SELECT SUM(PROFIT_MARGIN) -- +1592983
FROM ATLIQ_MASTER_TABLE
WHERE PROFIT_MARGIN > 0; -- 77883 Rows

-- Show Transactions for Delhi NCR & Chennai market (Market_Code: Mark001 and Mark004)

SELECT *
FROM ATLIQ_MASTER_TABLE
WHERE MARKET_CODE IN ('Mark001', 'Mark004');

-- Show distrinct product codes that were sold in Delhi NCR and Chennai;

SELECT DISTINCT(product_code), markets_name
FROM ATLIQ_MASTER_TABLE
WHERE MARKET_CODE IN ('Mark001', 'Mark004')
ORDER BY markets_name DESC;

-- Show Total sales in Delhi NCR and Chennai;

SELECT markets_name, SUM(SALES_AMOUNT) AS TOTAL_SALES
FROM ATLIQ_MASTER_TABLE
WHERE MARKET_CODE IN ('Mark001', 'Mark004')
GROUP BY markets_name
ORDER BY markets_name DESC;


-- Show transactions only for US dollars
SELECT *
FROM ATLIQ_MASTER_TABLE
WHERE CURRENCY = 'USD'; -- 2 ROWS

-- Coversion of USD to INR;
/*
SELECT DISTINCT(CURRENCY),
      CASE
        WHEN CURRENCY = 'USD' THEN 1
        ELSE 0
      END AS Convert_Currency
FROM ATLIQ_MASTER_TABLE;
*/

ALTER TABLE ATLIQ_MASTER_TABLE
ADD COLUMN Converted_Currency DECIMAL(10,2);

UPDATE ATLIQ_MASTER_TABLE
SET Converted_Currency = CASE CURRENCY
                           WHEN 'USD' THEN sales_amount * (81.75) -- AS ON 29th Jan, 2023
                           WHEN 'INR' THEN sales_amount
                         END;

SELECT CURRENCY, Converted_Currency
FROM ATLIQ_MASTER_TABLE
WHERE CURRENCY = 'USD';                       

-- Show total revenue for all years and markets_name;

SELECT YEAR, MARKETS_NAME, SUM(SALES_AMOUNT) AS  TOTAL_REVENUE
FROM ATLIQ_MASTER_TABLE
GROUP BY YEAR, MARKETS_NAME
ORDER BY YEAR;

-- Show total revenue in all years only for January Month

SELECT YEAR, MONTH_NAME, SUM(SALES_AMOUNT) AS TOTAL_REVENUE
FROM ATLIQ_MASTER_TABLE
WHERE MONTH_NAME = 'January'
GROUP BY YEAR, MONTH_NAME;

-- Show total revenue for all all the product types in particular year.

SELECT PRODUCT_TYPE, YEAR, SUM(SALES_AMOUNT) AS TOTAL_REVENUE
FROM ATLIQ_MASTER_TABLE
GROUP BY PRODUCT_TYPE, YEAR;

-- Answer of Why I used Left Join;

SELECT PRODUCT_CODE, CUSTOMER_CODE, PRODUCT_TYPE, YEAR, SUM(SALES_AMOUNT) AS TOTAL_REVENUE
FROM ATLIQ_MASTER_TABLE
GROUP BY PRODUCT_TYPE, YEAR, PRODUCT_CODE, CUSTOMER_CODE;

-- Now, Connecting this database to PowerBI.

-- MY Snowflake Server: lzfkniu-pv70345.snowflakecomputing.com

-- After Loading the dataset into PowerBi. Need some changes; -- Direct Query (Live Connection Data);

ALTER TABLE VJ_DATE
MODIFY COLUMN YEAR SET DATA TYPE INTEGER;                     

-- ********************************************************************* --

CREATE OR REPLACE TABLE CONSUM_COMPLAINTS_COPY AS
SELECT * FROM CONSUM_COMPLAINTS;

##### LAODING A FILE FROM EXTERNAL STAGE - AWS ##########

CREATE OR REPLACE TABLE CONSUM_COMPLAINTS_AWS LIKE CONSUM_COMPLAINTS;

SHOW COLUMNS IN CONSUM_COMPLAINTS_AWS;

--REMOVE FILES
REMOVE @AWS_S3_STORAGE_SERVICE;

--
##COPYING INTO STORAGE SERVICE
COPY INTO CONSUM_COMPLAINTS_AWS FROM @AWS_S3_STORAGE_SERVICE
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
PURGE = TRUE;

--BINGO DONE....
SELECT * FROM CONSUM_COMPLAINTS_AWS;
