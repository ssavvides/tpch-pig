 SET default_parallel $reducers;

supplier = load '$input/supplier' USING PigStorage('|') as (s_suppkey:long, s_name:chararray, s_address:chararray, s_nationkey:int, s_phone:chararray, s_acctbal:double, s_comment:chararray);

lineitem0 = load '$input/lineitem' USING PigStorage('|') as (l_orderkey:long, l_partkey:long, l_suppkey:long, l_linenumber:long, l_quantity:double, l_extendedprice:double, l_discount:double, l_tax:double, l_returnflag:chararray, l_linestatus:chararray, l_shipdate:chararray, l_commitdate:chararray, l_receiptdate:chararray,l_shippingstruct:chararray, l_shipmode:chararray, l_comment:chararray);

orders = load '$input/orders' USING PigStorage('|') as (o_orderkey:long, o_custkey:long, o_orderstatus:chararray, o_totalprice:double, o_orderdate:chararray, o_orderpriority:chararray, o_clerk:chararray, o_shippriority:long, o_comment:chararray);

customer = load '$input/customer' USING PigStorage('|') as (c_custkey:long,c_name:chararray, c_address:chararray, c_nationkey:int, c_phone:chararray, c_acctbal:double, c_mktsegment:chararray, c_comment:chararray);


nation10 = load '$input/nation' USING PigStorage('|') as (n_nationkey:int, n_name1:chararray, n_regionkey:int, n_comment:chararray);

nation20 = load '$input/nation' USING PigStorage('|') as (n_nationkey:int, n_name2:chararray, n_regionkey:int, n_comment:chararray);

nation1 = filter nation10 by n_name1=='FRANCE' or n_name1=='GERMANY';
nation2 = filter nation20 by n_name2=='FRANCE' or n_name2=='GERMANY';

lineitem = filter lineitem0 by l_shipdate >= '1995-01-01' and l_shipdate <= '1996-12-31';

supplier_nation1 = join supplier by s_nationkey, nation1 by n_nationkey USING 'replicated';

liteitem_supplier_nation1 = join supplier_nation1 by s_suppkey, lineitem by l_suppkey; -- big relaion on right.

customer_nation2 = join customer by c_nationkey, nation2 by n_nationkey USING 'replicated';

orders_customer_nation2 = join customer_nation2 by c_custkey, orders by o_custkey; -- big relaion on right.

final_join = join orders_customer_nation2 by o_orderkey, liteitem_supplier_nation1 by l_orderkey; -- big relaion on right.

filtered_final_join = filter final_join by 
(n_name1=='FRANCE' and n_name2=='GERMANY') or 
(n_name1=='GERMANY' and n_name2=='FRANCE');

shipping = foreach filtered_final_join GENERATE 
	n_name1 as supp_nation, 
	n_name2 as cust_nation, 
	SUBSTRING(l_shipdate, 0, 4) as l_year, l_extendedprice * (1 - l_discount) as volume;

grouped_shipping = group shipping by (supp_nation, cust_nation, l_year);
aggregated_shipping = foreach grouped_shipping GENERATE FLATTEN(group), SUM($1.volume) as revenue;

ordered_shipping = order aggregated_shipping by group::supp_nation, group::cust_nation, group::l_year;
store ordered_shipping into '$output/Q7out' USING PigStorage('|');
