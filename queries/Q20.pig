SET default_parallel $reducers;

part = load '$input/part' USING PigStorage('|') as (p_partkey:long, p_name:chararray, p_mfgr:chararray, p_brand:chararray, p_type:chararray, p_size:long, p_container:chararray, p_retailprice:double, p_comment:chararray);
part1 = foreach part generate p_partkey, p_name;
part2 = filter part1 by SUBSTRING(p_name, 0, 6)=='forest';
part3 = foreach part2 generate p_partkey;
part4 = distinct part3;

lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey, l_partkey:long, l_suppkey:long, l_linenumber, l_quantity:double, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate:chararray, l_commitdate, l_receiptdate,l_shippingstruct, l_shipmode, l_comment);
lineitem1 = foreach lineitem generate l_partkey, l_suppkey, l_shipdate, l_quantity;
lineitem2 = filter lineitem1 by l_shipdate >= '1994-01-01' and l_shipdate < '1995-01-01';
lineitem3 = group lineitem2 by (l_partkey, l_suppkey);
lineitem4 = foreach lineitem3 generate FLATTEN(group), SUM(lineitem2.l_quantity) * 0.5 as sum;

partsupp = load '$input/partsupp' USING PigStorage('|') as (ps_partkey:long, ps_suppkey:long, ps_availqty:long, ps_supplycost, ps_comment);
partsupp1 = foreach partsupp generate ps_suppkey, ps_partkey, ps_availqty;
ps_p = join part4 by p_partkey, partsupp1 by ps_partkey;
l_ps_p = join ps_p by (ps_partkey, ps_suppkey), lineitem4 by (l_partkey, l_suppkey);
l_ps_p2 = filter l_ps_p by ps_availqty > sum;
ps_suppkey_prj = foreach l_ps_p2 generate ps_suppkey;
ps_suppkey_distinct = distinct ps_suppkey_prj;

nation = load '$input/nation' USING PigStorage('|') as (n_nationkey:int, n_name:chararray, n_regionkey:int, n_comment:chararray);
nation1 = filter nation by n_name == 'CANADA';

supplier = load '$input/supplier' USING PigStorage('|') as (s_suppkey:long, s_name:chararray, s_address:chararray, s_nationkey:int, s_phone, s_acctbal, s_comment);
supplier1 = foreach supplier generate s_suppkey,s_name,s_nationkey,s_address;
s_n = join supplier by s_nationkey, nation1 by n_nationkey;
s_n_ps = join s_n by s_suppkey, ps_suppkey_distinct by ps_suppkey;

res_prj = foreach s_n_ps generate s_name,s_address;
res = order res_prj by s_name;

store res into '$output/Q20out' USING PigStorage('|');


