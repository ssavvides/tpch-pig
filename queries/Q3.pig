
SET default_parallel $reducers;

customer = load '$input/customer' USING PigStorage('|') as (c_custkey:long,c_name:chararray, c_address:chararray, c_nationkey:int, c_phone:chararray, c_acctbal:double, c_mktsegment:chararray, c_comment:chararray);

orders = load '$input/orders' USING PigStorage('|') as (o_orderkey:long, o_custkey:long, o_orderstatus:chararray, o_totalprice:double, o_orderdate:chararray, o_orderpriority:chararray, o_clerk:chararray, o_shippriority:long, o_comment:chararray);

lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long, l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shippingstruct:chararray, l_shipmode:chararray, l_comment:chararray);


fcustomer = filter customer by c_mktsegment == 'BUILDING';
forders = filter orders by o_orderdate < '1995-03-15';
flineitem = filter lineitem by l_shipdate > '1995-03-15';

o1 = join forders by o_custkey, fcustomer by c_custkey;
selo1 = foreach o1 generate o_orderkey, o_orderdate, o_shippriority;

l1 = join flineitem by l_orderkey, selo1 by o_orderkey;
sell1 = foreach l1 generate l_orderkey, l_extendedprice*(1-l_discount) as volume, o_orderdate, o_shippriority;

grResult = group sell1 by (l_orderkey, o_orderdate, o_shippriority);
sumResult = foreach grResult generate flatten(group), SUM(sell1.volume) as revenue;
sortResult = order sumResult by revenue desc, o_orderdate;
limitResult = limit sortResult 10;

store limitResult into '$output/Q3out' USING PigStorage('|');
