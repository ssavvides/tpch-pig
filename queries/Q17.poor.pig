SET default_parallel $reducers;

lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long, l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shippingstruct:chararray, l_shipmode:chararray, l_comment:chararray);

part = load '$input/part' USING PigStorage('|') as (p_partkey:long, p_name:chararray, p_mfgr:chararray, p_brand:chararray, p_type:chararray, p_size:long, p_container:chararray, p_retailprice:double, p_comment:chararray);

g_lineitem = group lineitem by l_partkey;
lineitem2 = foreach g_lineitem generate FLATTEN( lineitem ), 0.2 * AVG(lineitem.l_quantity) as l_avg;
lineitem3 = filter lineitem2 by l_quantity < l_avg;

part1 = FILTER part BY p_brand == 'Brand#23' AND p_container == 'MED BOX';
p_l = join lineitem3 by l_partkey, part1 by p_partkey;
g2 = group p_l ALL;
result = foreach g2 generate SUM(p_l.l_extendedprice)/7.0;

STORE result INTO '$output/Q17poor_out' USING PigStorage('|');
