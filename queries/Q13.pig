SET default_parallel $reducers;

customer = load '$input/customer' USING PigStorage('|') as (c_custkey:long,c_name:chararray, c_address:chararray, c_nationkey:int, c_phone:chararray, c_acctbal:double, c_mktsegment:chararray, c_comment:chararray);

orders = load '$input/orders' USING PigStorage('|') as (o_orderkey:long, o_custkey:long, o_orderstatus:chararray, o_totalprice:double, o_orderdate:chararray, o_orderpriority:chararray, o_clerk:chararray, o_shippriority:long, o_comment:chararray);

forders = FILTER orders by (NOT o_comment MATCHES '.*special.*requests.*');

porders = FOREACH forders GENERATE o_custkey, o_orderkey;

pcustomer = FOREACH customer GENERATE c_custkey;

COG1 = COGROUP pcustomer by c_custkey, porders by o_custkey;
COG2 = filter COG1 by COUNT(pcustomer) > 0; -- left out join, ensure left side non-empty
COG = FOREACH COG2 GENERATE group as c_custkey, COUNT(porders.o_orderkey) as c_count;

groupResult = GROUP COG BY c_count;

countResult = FOREACH groupResult GENERATE group as c_count, COUNT(COG) as custdist;

orderResult = ORDER countResult by custdist DESC, c_count DESC;

store orderResult into '$output/Q13out' USING PigStorage('|');
