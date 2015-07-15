SET default_parallel $reducers;

orders = load '$input/orders' USING PigStorage('|') as (o_orderkey:long, o_custkey:long, o_orderstatus:chararray, o_totalprice:double, o_orderdate:chararray, o_orderpriority:chararray, o_clerk:chararray, o_shippriority:long, o_comment:chararray);

lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long, l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shippingstruct:chararray, l_shipmode:chararray, l_comment:chararray);

supplier = load '$input/supplier' USING PigStorage('|') as (s_suppkey:long, s_name:chararray, s_address:chararray, s_nationkey:int, s_phone:chararray, s_acctbal:double, s_comment:chararray);

nation = load '$input/nation' USING PigStorage('|') as (n_nationkey:int, n_name:chararray, n_regionkey:int, n_comment:chararray);

--------------------------
lineitem = foreach lineitem generate l_suppkey,l_orderkey,l_receiptdate,l_commitdate;
orders = foreach orders generate o_orderkey, o_orderstatus;
supplier = foreach supplier generate s_suppkey, s_nationkey, s_name;

gl = group lineitem by l_orderkey;

L2 = filter gl by COUNT(org.apache.pig.builtin.Distinct(lineitem.l_suppkey))>1;

fL2 = foreach L2{
        t1 = filter lineitem by l_receiptdate > l_commitdate;
        generate group, t1;
}
-- for some reason we store fL2 here otherwise the result is wrong. 
store fL2 into '$output/Q21_fL2_fL2' using PigStorage('|');

fL3 = filter fL2 by COUNT(org.apache.pig.builtin.Distinct($1.l_suppkey)) == 1;

L3 = foreach fL3 generate flatten($1);

-----------------------

fn = filter nation by n_name == 'SAUDI ARABIA';
fn_s = join supplier by s_nationkey, fn by n_nationkey USING 'replicated';

fn_s_L3 = join L3 by l_suppkey, fn_s by s_suppkey;

fo = filter orders by o_orderstatus == 'F';
fn_s_L3_fo = join fn_s_L3 by l_orderkey, fo by o_orderkey;

gres = group fn_s_L3_fo by s_name;
sres = foreach gres generate group as s_name, COUNT($1) as numwait; 
ores = order sres by numwait desc, s_name;
lres = limit ores 100;

store lres into '$output/Q21out' using PigStorage('|');

fs -rmr $output/Q21_fL2_fL2

