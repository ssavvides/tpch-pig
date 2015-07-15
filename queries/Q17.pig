SET default_parallel $reducers;

lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long, l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shippingstruct:chararray, l_shipmode:chararray, l_comment:chararray);

part = load '$input/part' USING PigStorage('|') as (p_partkey:long, p_name:chararray, p_mfgr:chararray, p_brand:chararray, p_type:chararray, p_size:long, p_container:chararray, p_retailprice:double, p_comment:chararray);

lineitem = foreach lineitem generate l_partkey, l_quantity, l_extendedprice ;
part = FILTER part BY p_brand == 'Brand#23' AND p_container == 'MED BOX';
part = foreach part generate p_partkey;

COG1 = COGROUP part by p_partkey, lineitem by l_partkey;
COG1 = filter COG1 by COUNT(part) > 0;
COG2 = FOREACH COG1 GENERATE COUNT(part) as count_part, FLATTEN(lineitem), 0.2 * AVG(lineitem.l_quantity) as l_avg; 

COG3 = filter COG2 by l_quantity < l_avg;
COG = foreach COG3 generate (l_extendedprice * count_part) as l_sum;

G1 = group COG ALL;

result = foreach G1 generate SUM(COG.l_sum)/7.0;

STORE result INTO '$output/Q17out' USING PigStorage('|');
