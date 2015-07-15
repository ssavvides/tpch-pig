SET default_parallel $reducers;
Orders = LOAD '$input/orders' USING PigStorage('|') AS (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority:chararray, o_clerk, o_shippriority, o_comment);

LineItem = LOAD '$input/lineitem' USING PigStorage('|') AS (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment);
DateFilter = FILTER Orders BY o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01';
CommitDateFilter = FILTER LineItem BY l_commitdate < l_receiptdate;
OrdersLineItem = JOIN DateFilter BY o_orderkey, CommitDateFilter BY l_orderkey;
OrdersLineItemAll = GROUP OrdersLineItem BY o_orderkey;
OrdersCount = FOREACH OrdersLineItemAll GENERATE group, MIN(OrdersLineItem.o_orderpriority) as o_orderpriority;
PriorityGroup = GROUP OrdersCount BY o_orderpriority;
PriorityChecking = FOREACH PriorityGroup GENERATE group, COUNT(OrdersCount) AS order_count;

SortedPriorityChecking = ORDER PriorityChecking BY group;
STORE SortedPriorityChecking into '$output/Q4poor_out';

