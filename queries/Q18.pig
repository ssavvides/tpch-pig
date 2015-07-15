SET default_parallel $reducers;

lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long, l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shippingstruct:chararray, l_shipmode:chararray, l_comment:chararray);

customer = load '$input/customer' USING PigStorage('|') as (c_custkey:long,c_name:chararray, c_address:chararray, c_nationkey:int, c_phone:chararray, c_acctbal:double, c_mktsegment:chararray, c_comment:chararray);

orders = load '$input/orders' USING PigStorage('|') as (o_orderkey:long, o_custkey:long, o_orderstatus:chararray, o_totalprice:double, o_orderdate:chararray, o_orderpriority:chararray, o_clerk:chararray, o_shippriority:long, o_comment:chararray);

lineitem = foreach lineitem generate l_orderkey, l_quantity;
orders = foreach orders generate o_orderkey,o_orderdate, o_totalprice, o_custkey;
COG = cogroup lineitem by l_orderkey, orders by o_orderkey;

COG = filter COG by SUM(lineitem.l_quantity) > 300;

lineitem_orders = foreach COG generate SUM(lineitem.l_quantity) as l_quantity_sum, flatten(orders); -- orders has only one tuple per bag!

lineitem_orders_customer = join customer by c_custkey, lineitem_orders by o_custkey;

lineitem_orders_customer_group = group lineitem_orders_customer by (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice);
result = foreach lineitem_orders_customer_group generate group.c_name as c_name, group.c_custkey as c_custkey, group.o_orderkey as o_orderkey, group.o_orderdate as o_orderdate, group.o_totalprice as o_totalprice, SUM(lineitem_orders_customer.l_quantity_sum) as l_quantity_sum;

out = order result by o_totalprice desc, o_orderdate;

store out into '$output/Q18out' USING PigStorage('|');
