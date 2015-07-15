SET default_parallel $reducers;

lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long, l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shippingstruct:chararray, l_shipmode:chararray, l_comment:chararray);

part = load '$input/part' USING PigStorage('|') as (p_partkey:long, p_name:chararray, p_mfgr:chararray, p_brand:chararray, p_type:chararray, p_size:long, p_container:chararray, p_retailprice:double, p_comment:chararray);

-- filter first
filtered_part = FILTER part BY p_brand == 'Brand#23' AND p_container == 'MED BOX';

-- COGROUP by part key ... result is (partkey), BAG(partkey,orderkey,.....), BAG(partkey, name, ...)
join_line_part = join lineitem BY l_partkey, filtered_part by p_partkey;
group_line_part = group join_line_part by p_partkey;

-- FOREACH to only project partkey and 0.2 * avg(quantity)
project_lp = FOREACH group_line_part GENERATE group AS partkey, 0.2 * AVG($1.l_quantity) AS avg_quantity;

-- job 3: another join: result is partkey, orderkey, suppkey, ..., partkey, avg_quantity
join_line_project_lp = join lineitem BY l_partkey, project_lp BY partkey;

-- FILTER quantity < avg_quantity
filtered_join_line_project_lp = FILTER join_line_project_lp BY l_quantity < avg_quantity;

--job 4: Aggregate the extended_price
grouped_filtered = GROUP filtered_join_line_project_lp ALL;

result = FOREACH grouped_filtered GENERATE SUM($1.l_extendedprice) / 7 as avg_yearly;

STORE result INTO '$output/Q17poorer_out' USING PigStorage('|');
