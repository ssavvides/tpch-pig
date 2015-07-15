 SET default_parallel $reducers;

customer = load '$input/customer' USING PigStorage('|') as (c_custkey:long,c_name:chararray, c_address:chararray, c_nationkey:int, c_phone:chararray, c_acctbal:double, c_mktsegment:chararray, c_comment:chararray);
orders = load '$input/orders' USING PigStorage('|') as (o_orderkey:long, o_custkey:long, o_orderstatus:chararray, o_totalprice:double, o_orderdate:chararray, o_orderpriority:chararray, o_clerk:chararray, o_shippriority:long, o_comment:chararray);

customer_filter = filter customer by c_acctbal>0.00 and SUBSTRING(c_phone, 0, 2) MATCHES '13|31|23|29|30|18|17';
customer_filter_group = group customer_filter all;
avg_customer_filter = foreach customer_filter_group generate AVG(customer_filter.c_acctbal) as avg_c_acctbal;

customer_sec_filter = filter customer by c_acctbal > avg_customer_filter.avg_c_acctbal and SUBSTRING(c_phone, 0, 2) MATCHES '13|31|23|29|30|18|17';
customer_orders_left = join customer_sec_filter by c_custkey left, orders by o_custkey;

customer_trd_filter = filter customer_orders_left by o_custkey is null;
customer_rows = foreach customer_trd_filter generate SUBSTRING(c_phone, 0, 2) as cntrycode, c_acctbal;

customer_result_group = group customer_rows by cntrycode;
customer_result = foreach customer_result_group generate group, COUNT(customer_rows) as numcust, SUM(customer_rows.c_acctbal) as totacctbal;
customer_result_inorder = order customer_result by group;

store customer_result_inorder into '$output/Q22out' USING PigStorage('|');
