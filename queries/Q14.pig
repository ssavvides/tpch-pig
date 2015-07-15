SET default_parallel $reducers;

lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long, l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shippingstruct:chararray, l_shipmode:chararray, l_comment:chararray);

part = load '$input/part' USING PigStorage('|') as (p_partkey:long, p_name:chararray, p_mfgr:chararray, p_brand:chararray, p_type:chararray, p_size:long, p_container:chararray, p_retailprice:double, p_comment:chararray);

filtered_lineitem = filter lineitem by l_shipdate >= '1995-09-01' and l_shipdate < '1995-10-01';
lineitem2 = foreach filtered_lineitem generate l_partkey, l_extendedprice * (1 - l_discount) as l_value;

lineitem_part = join lineitem2 by l_partkey, part by p_partkey;
lineitem_part_grouped = group lineitem_part ALL;
sum_all = foreach lineitem_part_grouped generate SUM(lineitem_part.l_value);

f_lineitem_part = filter lineitem_part by SUBSTRING(p_type, 0, 5)=='PROMO';
f_lineitem_part_group = group f_lineitem_part ALL;
sum_filter = foreach f_lineitem_part_group generate SUM(f_lineitem_part.l_value);

promo_revenue = foreach sum_all generate 100*sum_filter.$0/sum_all.$0;

store promo_revenue into '$output/Q14out' USING PigStorage('|');
