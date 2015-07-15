
SET default_parallel $reducers;

lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long,
l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shippingstruct:chararray, l_shipmode:chararray, l_comment:chararray);

flineitem = FILTER lineitem BY l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount >= 0.05  AND l_discount <= 0.07 AND l_quantity < 24;

saving = FOREACH flineitem GENERATE l_extendedprice * l_discount;
grpResult = GROUP saving ALL;
sumResult = FOREACH grpResult GENERATE SUM(saving);

store sumResult into '$output/Q6poor_out' USING PigStorage('|');
