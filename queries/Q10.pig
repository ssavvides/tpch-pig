
SET default_parallel $reducers;

customer = load '$input/customer' USING PigStorage('|') as (c_custkey:long,c_name:chararray, c_address:chararray, c_nationkey:int, c_phone:chararray, c_acctbal:double, c_mktsegment:chararray, c_comment:chararray);

orders = load '$input/orders' USING PigStorage('|') as (o_orderkey:long, o_custkey:long, o_orderstatus:chararray, o_totalprice:double, o_orderdate:chararray, o_orderpriority:chararray, o_clerk:chararray, o_shippriority:long, o_comment:chararray);

lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long, l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shippingstruct:chararray, l_shipmode:chararray, l_comment:chararray);

supplier = load '$input/supplier' USING PigStorage('|') as (s_suppkey:long, s_name:chararray, s_address:chararray, s_nationkey:int, s_phone:chararray, s_acctbal:double, s_comment:chararray);

nation = load '$input/nation' USING PigStorage('|') as (n_nationkey:int, n_name:chararray, n_regionkey:int, n_comment:chararray);

region = load '$input/region' USING PigStorage('|') as (r_regionkey:int, r_name:chararray, r_comment:chararray);

partsupp = load '$input/partsupp' USING PigStorage('|') as (ps_partkey:long, ps_suppkey:long, ps_availqty:long, ps_supplycost:double, ps_comment:chararray);

part = load '$input/part' USING PigStorage('|') as (p_partkey:long, p_name:chararray, p_mfgr:chararray, p_brand:chararray, p_type:chararray, p_size:long, p_container:chararray, p_retailprice:double, p_comment:chararray);


forders = filter orders by o_orderdate < '1994-01-01' and o_orderdate >= '1993-10-01';
flineitem = filter lineitem by l_returnflag == 'R';

c1 = join customer by c_custkey, forders by o_custkey;
selc1 = foreach c1 generate c_custkey, c_name, c_acctbal, c_address, c_phone, c_comment, c_nationkey, o_orderkey;

o1 = join nation by n_nationkey, selc1 by c_nationkey;
selo1 = foreach o1 generate o_orderkey, c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, n_name;

l1 = join flineitem by l_orderkey, selo1 by o_orderkey;
sell1 = foreach l1 generate c_custkey, c_name, l_extendedprice * (1 - l_discount) as volume, c_acctbal, n_name, c_address, c_phone, c_comment;

grResult = GROUP sell1 by (c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment);

sumResult = foreach grResult generate group.c_custkey, group.c_name, SUM(sell1.volume) as revenue, group.c_acctbal, group.n_name, group.c_address, group.c_phone, group.c_comment;
sortResult = order sumResult by revenue desc;
limitResult = limit sortResult 20;

store limitResult into '$output/Q10out' USING PigStorage('|');
