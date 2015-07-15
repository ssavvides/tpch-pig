
SET default_parallel $reducers;

lineitem = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long, l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shipinstruct:chararray, l_shipmode:chararray, l_comment:chararray);

part = load '$input/part' USING PigStorage('|') as (p_partkey:long, p_name:chararray, p_mfgr:chararray, p_brand:chararray, p_type:chararray, p_size:long, p_container:chararray, p_retailprice:double, p_comment:chararray);

lpart = JOIN lineitem BY l_partkey, part by p_partkey;

fltResult = FILTER lpart BY 
  (
    p_brand == 'Brand#12'
	and p_container matches 'SM CASE|SM BOX|SM PACK|SM PKG'
	and l_quantity >= 1 and l_quantity <= 11
	and p_size >= 1 and p_size <= 5
	and l_shipmode matches 'AIR|AIR REG'
	and l_shipinstruct == 'DELIVER IN PERSON'
  ) 
  or 
  (
    p_brand == 'Brand#23'
	and p_container matches 'MED BAG|MED BOX|MED PKG|MED PACK'
	and l_quantity >= 10 and l_quantity <= 20
	and p_size >= 1 and p_size <= 10
	and l_shipmode matches 'AIR|AIR REG'
	and l_shipinstruct == 'DELIVER IN PERSON'
  )
  or
  (
	p_brand == 'Brand#34'
	and p_container matches 'LG CASE|LG BOX|LG PACK|LG PKG'
	and l_quantity >= 20 and l_quantity <= 30
	and p_size >= 1 and p_size <= 15
	and l_shipmode matches 'AIR|AIR REG'
	and l_shipinstruct == 'DELIVER IN PERSON'
  );
volume = FOREACH fltResult GENERATE l_extendedprice * (1 - l_discount);
grpResult = GROUP volume ALL;
revenue = FOREACH grpResult GENERATE SUM(volume);

store revenue into '$output/Q19out' USING PigStorage('|');


