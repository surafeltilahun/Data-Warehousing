create or replace PROCEDURE INLJ IS
-- Declare variables --
v_tuples      NUMBER := 50;
v_tuples_from NUMBER := 1;
v_tuples_to   NUMBER := 1;
v_helper_200    NUMBER := 1;
v_helper_50    NUMBER := 1;

--Declare a cursor for masterdata--
CURSOR  c_products (p_product_id masterdata.product_id%TYPE) IS
SELECT  m.*
FROM    masterdata  m
WHERE   m.product_id = p_product_id;

--Declare a cursor for transaction--
CURSOR  c_transaction IS
SELECT  t.*
FROM    transactions t;

--Declare the cursor data type --
TYPE get_type IS TABLE OF c_transaction%ROWTYPE;
c_record get_type;  
r_products  c_products%ROWTYPE;

--Open the cursor and fetch rows from transaction table--
BEGIN
    OPEN c_transaction;
    FETCH c_transaction BULK COLLECT INTO c_record;
    CLOSE c_transaction;

--Begin the First Loop (200 iterations)--  
    FOR v_helper_200 IN 1..200
    LOOP 
    --Begin the second loop and insert 50 tuples on row by row basis--
        FOR v_helper_50 IN v_tuples_from..v_tuples_to
        LOOP
            OPEN c_products (c_record(v_tuples_from).product_id);
            FETCH c_products INTO r_products;
            CLOSE c_products;

            INSERT INTO dim_store ( store_name, store_id) 
            SELECT  c_record(v_tuples_from).store_name,c_record(v_tuples_from).store_id
            FROM    dual   
            WHERE   c_record(v_tuples_from).store_id NOT IN 
                                                    (SELECT s.store_id 
                                                     FROM dim_store s 
                                                     WHERE s.store_id = c_record(v_tuples_from).store_id);
            INSERT INTO dim_supplier (supplier_id, supplier_name) 
            SELECT  r_products.supplier_id, r_products.supplier_name
            FROM    dual   
            WHERE   r_products.supplier_id NOT IN 
                                                    (SELECT s.supplier_id 
                                                     FROM dim_supplier s 
                                                     WHERE s.supplier_id = r_products.supplier_id );
            INSERT INTO dim_product (product_id, product_name, product_price) 
            SELECT  r_products.product_id, r_products.product_name, r_products.price
            FROM    dual   
            WHERE   r_products.product_id NOT IN     (SELECT p.product_id 
                                                       FROM dim_product p 
                                                       WHERE p.product_id = r_products.product_id );
            INSERT INTO dim_customer (customer_id, customer_name) 
            SELECT  c_record(v_tuples_from).customer_id, c_record(v_tuples_from).customer_name
            FROM    dual   
            WHERE   c_record(v_tuples_from).customer_id NOT IN 
                                                       (SELECT c.customer_id 
                                                        FROM dim_customer c 
                                                        WHERE c.customer_id = c_record(v_tuples_from).customer_id);

            INSERT INTO dim_time(t_date, t_year, t_month, t_quarter)
            SELECT  c_record(v_tuples_from).t_date, 
                     TO_CHAR(c_record(v_tuples_from).t_date,'YYYY'),
                     TO_CHAR(c_record(v_tuples_from).t_date,'MM'),
                     TO_CHAR(c_record(v_tuples_from).t_date,'Q')
                    
            FROM    dual   
            WHERE   c_record(v_tuples_from).t_date NOT IN
                                                (SELECT t.t_date 
                                                 FROM dim_time t 
                                                 WHERE t.t_date = c_record(v_tuples_from).t_date);

            INSERT INTO fact_sales(store_id, supplier_id, product_id, customer_id, transaction_id, t_date, quantity,total_sales) 
            SELECT  c_record(v_tuples_from).store_id, r_products.supplier_id, r_products.product_id, 
                    c_record(v_tuples_from).customer_id, c_record(v_tuples_from).transaction_id, 
                    c_record(v_tuples_from).t_date, c_record(v_tuples_from).quantity, 
                    (c_record(v_tuples_from).quantity * r_products.price)
            FROM    dual    
            WHERE   c_record(v_tuples_from).transaction_id NOT IN 
                                                          (SELECT fs.transaction_id 
                                                           FROM fact_sales fs 
                                                           WHERE fs.transaction_id = c_record(v_tuples_from).transaction_id);

            v_tuples_from := v_tuples_from + 1;
            v_tuples_to := v_tuples_to + 1;
        END LOOP;
    END LOOP;
    COMMIT;
END;
