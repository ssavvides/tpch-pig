
SET default_parallel $reducers;

customer = load '$input/customer' USING PigStorage('|') as (c_custkey:long,c_name:chararray, c_address:chararray, c_nationkey:int, c_phone:chararray, c_acctbal:double, c_mktsegment:chararray, c_comment:chararray);

orders = load '$input/orders' USING PigStorage('|') as (o_orderkey:long, o_custkey:long, o_orderstatus:chararray, o_totalprice:double, o_orderdate:chararray, o_orderpriority:chararray, o_clerk:chararray, o_shippriority:long, o_comment:chararray);

lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long, l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shippingstruct:chararray, l_shipmode:chararray, l_comment:chararray);

supplier = load '$input/supplier' USING PigStorage('|') as (s_suppkey:long, s_name:chararray, s_address:chararray, s_nationkey:int, s_phone:chararray, s_acctbal:double, s_comment:chararray);

nation = load '$input/nation' USING PigStorage('|') as (n_nationkey:int, n_name:chararray, n_regionkey:int, n_comment:chararray);

region = load '$input/region' USING PigStorage('|') as (r_regionkey:int, r_name:chararray, r_comment:chararray);

part = load '$input/part' USING PigStorage('|') as (p_partkey:long, p_name:chararray, p_mfgr:chararray, p_brand:chararray, p_type:chararray, p_size:long, p_container:chararray, p_retailprice:double, p_comment:chararray);
fregion = filter region by r_name == 'AMERICA';
forders = filter orders by o_orderdate <= '1996-12-31' and o_orderdate >= '1995-01-01';
fpart = filter part by p_type == 'ECONOMY ANODIZED STEEL';

n1 = join nation by n_regionkey, fregion by r_regionkey USING 'replicated';
seln1 = foreach n1 generate n_nationkey;

c1 = join customer by c_nationkey, seln1 by n_nationkey USING 'replicated';
selc1 = foreach c1 generate c_custkey;

o1 = join forders by o_custkey, selc1 by c_custkey;
selo1 = foreach o1 generate o_orderkey, o_orderdate;

n2 = join supplier by s_nationkey, nation by n_nationkey USING 'replicated';

lineitem = foreach lineitem generate l_partkey,l_suppkey,l_orderkey,l_extendedprice * (1 - l_discount) as volume;
p1 = join fpart by p_partkey, lineitem by l_partkey;

s1 = join n2 by s_suppkey, p1 by l_suppkey;

l1 = join o1 by o_orderkey, s1 by l_orderkey;

sels1 = foreach l1 generate SUBSTRING(o_orderdate,0,4) as o_year, volume, (n_name == 'BRAZIL'? volume : 0) as case_volume;
grResult = GROUP sels1 by o_year;

sumResult = foreach grResult generate group, SUM($1.case_volume) / SUM($1.volume);
sortResult = order sumResult by group;

store sortResult into '$output/Q8out' USING PigStorage('|');

