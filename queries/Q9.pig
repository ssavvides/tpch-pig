
SET default_parallel $reducers;

customer = load '$input/customer' USING PigStorage('|') as (c_custkey:long,c_name:chararray, c_address:chararray, c_nationkey:int, c_phone:chararray, c_acctbal:double, c_mktsegment:chararray, c_comment:chararray);

orders = load '$input/orders' USING PigStorage('|') as (o_orderkey:long, o_custkey:long, o_orderstatus:chararray, o_totalprice:double, o_orderdate:chararray, o_orderpriority:chararray, o_clerk:chararray, o_shippriority:long, o_comment:chararray);

lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long, l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shippingstruct:chararray, l_shipmode:chararray, l_comment:chararray);

supplier = load '$input/supplier' USING PigStorage('|') as (s_suppkey:long, s_name:chararray, s_address:chararray, s_nationkey:int, s_phone:chararray, s_acctbal:double, s_comment:chararray);

nation = load '$input/nation' USING PigStorage('|') as (n_nationkey:int, n_name:chararray, n_regionkey:int, n_comment:chararray);

region = load '$input/region' USING PigStorage('|') as (r_regionkey:int, r_name:chararray, r_comment:chararray);

partsupp = load '$input/partsupp' USING PigStorage('|') as (ps_partkey:long, ps_suppkey:long, ps_availqty:long, ps_supplycost:double, ps_comment:chararray);

part = load '$input/part' USING PigStorage('|') as (p_partkey:long, p_name:chararray, p_mfgr:chararray, p_brand:chararray, p_type:chararray, p_size:long, p_container:chararray, p_retailprice:double, p_comment:chararray);

fpart = filter part by REGEX_EXTRACT(p_name,'(green)', 1) != '';

j1 = join fpart by p_partkey, lineitem by l_partkey; -- 'replicated' failed again in 100GB.

j2 = join supplier by s_nationkey, nation by n_nationkey USING 'replicated';

j3 = join j2 by s_suppkey, j1 by l_suppkey;  -- 'replicated' failed in 100GB.

j4 = join partsupp by (ps_suppkey, ps_partkey), j3 by (l_suppkey, l_partkey);

j5 = join j4 by l_orderkey, orders by o_orderkey;

selo1 = foreach j5 generate n_name as nation_name, SUBSTRING(o_orderdate, 0, 4) as o_year, (l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as amount;

grResult = GROUP selo1 by (nation_name, o_year);

sumResult = foreach grResult generate flatten(group), SUM(selo1.amount) as sum_profit;
sortResult = order sumResult by nation_name, o_year desc;

store sortResult into '$output/Q9out' USING PigStorage('|');
