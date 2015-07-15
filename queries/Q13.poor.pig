SET default_parallel $reducers;

-- loading data 

customer = load '$input/customer' USING PigStorage('|') as (c_custkey:long,c_name:chararray, c_address:chararray, c_nationkey:int, c_phone:chararray, c_acctbal:double, c_mktsegment:chararray, c_comment:chararray);

orders = load '$input/orders' USING PigStorage('|') as (o_orderkey:long, o_custkey:long, o_orderstatus:chararray, o_totalprice:double, o_orderdate:chararray, o_orderpriority:chararray, o_clerk:chararray, o_shippriority:long, o_comment:chararray);

forders = FILTER orders by (NOT o_comment MATCHES '.*special.*requests.*');

porders = FOREACH forders GENERATE o_custkey, o_orderkey;

pcustomer = FOREACH customer GENERATE c_custkey;

custorder = join pcustomer by c_custkey LEFT OUTER, porders by o_custkey;

gby = group custorder by c_custkey;

orderCounts = FOREACH gby
  GENERATE group as c_custkey, COUNT(custorder.o_orderkey) as c_count;

groupResult = GROUP orderCounts BY c_count;

countResult = FOREACH groupResult GENERATE group as c_count, COUNT(orderCounts) as custdist;

orderResult = ORDER countResult by custdist DESC, c_count DESC;

store orderResult into '$output/Q13poor_out' USING PigStorage('|');
