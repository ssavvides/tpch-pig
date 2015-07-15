SET default_parallel $reducers;

lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long, l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shippingstruct:chararray, l_shipmode:chararray, l_comment:chararray);

supplier = load '$input/supplier' USING PigStorage('|') as (s_suppkey:long, s_name:chararray, s_address:chararray, s_nationkey:int, s_phone:chararray, s_acctbal:double, s_comment:chararray);

flineitem = filter lineitem by l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01';

sumlineitem = foreach flineitem generate l_suppkey, l_extendedprice * (1 - l_discount) as value;

glineitem = group sumlineitem by l_suppkey;

revenue = foreach glineitem generate group as supplier_no, SUM($1.value) as total_revenue;

grevenue = group revenue all;

max_revenue = foreach grevenue generate MAX($1.total_revenue);

top_revenue = filter revenue by total_revenue == max_revenue.$0;

j1 = join supplier by s_suppkey, top_revenue by supplier_no;

sel = foreach j1 generate s_suppkey, s_name, s_address, s_phone, total_revenue;

ord = order sel by s_suppkey desc;
store ord into '$output/Q15out' USING  PigStorage('|');


