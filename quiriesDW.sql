--OLAP Query--

--Question 1--

SELECT
  *
FROM
(SELECT product_name,SUM(total_sales) AS total_sales,
DENSE_RANK() OVER (ORDER BY SUM(total_sales) DESC) AS RANK
FROM fact_sales fs
INNER JOIN dim_product USING(product_id)
INNER JOIN dim_time USING(t_date)
GROUP BY product_id,product_name)
WHERE RANK <=3;

--QuestiON 2--

SELECT * FROM
(SELECT store_name,SUM(total_sales) as Total_Sales,
RANK() OVER (ORDER BY SUM(total_sales) DESC) AS RANK
FROM fact_sales 
INNER JOIN dim_store USING (store_id) 
GROUP BY store_name,store_name)
WHERE RANK=1;

--QuestiON 3--

SELECT * FROM
(SELECT product_name AS PRODUC,supplier_name AS SUPPLIER_NAME,count(*) AS Num_trans, quantity AS QUANTITY_SOLD,
RANK() OVER (ORDER BY SUM (total_sales) DESC) AS RANK
FROM fact_sales 
INNER JOIN dim_product USING(product_id)
INNER JOIN dim_time d USING(t_date)
INNER JOIN dim_supplier USING(supplier_id)
WHERE d.t_year=2014
GROUP BY product_id, product_name, supplier_id, supplier_name,
quantity)
WHERE RANK=1;

--QuestiON 4--

WITH quarterlysale
AS
(
SELECT st.store_id, st.store_name, t.t_quarter, SUM(fs.total_sales) AS total_sales 
FROM  dim_time t, fact_sales fs, dim_store st
WHERE fs.store_id = st.store_id
AND t.t_year = 2014
AND t.t_date = fs.t_date
GROUP BY st.store_id, st.store_name,  t.t_quarter)

SELECT * 
    FROM
    (SELECT store_name,
    SUM(CASE WHEN t_quarter = 1 THEN total_sales ELSE 0 END) AS q1_2014,
    SUM(CASE WHEN t_quarter = 2 THEN total_sales ELSE 0 END) AS q2_2014,
    SUM(CASE WHEN t_quarter = 3 THEN total_sales ELSE 0 END) AS q3_2014,
    SUM(CASE WHEN t_quarter = 4 THEN total_sales ELSE 0 END) AS q4_2014
    FROM quarterlysale
    GROUP BY store_name)
    ORDER BY Store_name;

--Question 5--

WITH
previous_months_analysis AS
(   SELECT   &MONTH AS Inserted_Month
    FROM    dual),
personal_report AS
(SELECT  s.product_id, p.product_name, t.t_year, t.t_month, SUM(s.total_sales) AS sales_amount
FROM    fact_sales  s,
        dim_product p,
        dim_time    t,
        previous_months_analysis 
WHERE   s.product_id   =   p.product_id
AND     s.t_date    =   t.t_date
AND     t.t_year    =   &YEAR
AND     t.t_month   BETWEEN (Inserted_Month -2) AND Inserted_Month 
GROUP BY s.product_id, p.product_name, t.t_year, t.t_month
ORDER BY s.product_id, t.t_year, t.t_month),
RANK AS
(SELECT  pr.product_name,pr.t_year, pr.t_month, pr.sales_amount, RANK() OVER( PARTITION BY pr.t_year, pr.t_month ORDER BY pr.sales_amount DESC) AS RANK
    FROM    personal_report pr)
SELECT  *
FROM    RANK r,
        previous_months_analysis cu
WHERE   r.rank  <= 3;

--QuestiON 6--

DROP MATERIALIZED VIEW storeanalysis;
CREATE MATERIALIZED VIEW storeanalysis BUILD IMMEDIATE
REFRESH  COMPLETE
ENABLE QUERY REWRITE
AS
  SELECT fs.store_id AS "STOR",fs.product_id AS "PRODUC",SUM(fs.total_sales) AS "SUM(STORE_TOTAL)"
  FROM fact_sales fs
     GROUP BY fs.store_id,fs.product_id
     ORDER BY lpad(fs.store_id,4) ASC,fs.product_id;
     
select * from storeanalysis;

--QuestiON 7--

--rollup--

select STOR,decode(grouping(PRODUC),1,'Total:',to_char(PRODUC)) AS PRODUC,sum("SUM(STORE_TOTAL)") AS "SUM(TOTAL_SALES)"
FROM storeanalysis
GROUP BY  ROLLUP(STOR, PRODUC)
ORDER BY LPAD(STOR,4),PRODUC ASC;
