 SET default_parallel $reducers;

LineItems = LOAD '$input/lineitem' USING PigStorage('|') AS (orderkey:long, partkey:long, suppkey:long, linenumber:long, quantity:double, extendedprice:double, discount:double, tax:double, returnflag, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment);
SubLineItems = FILTER LineItems BY shipdate <= '1998-09-02';

SubLine = FOREACH SubLineItems GENERATE returnflag, linestatus, quantity, extendedprice, extendedprice*(1-discount) AS disc_price, extendedprice*(1-discount)*(1+tax) AS charge, discount;

StatusGroup = GROUP SubLine BY (returnflag, linestatus);
PriceSummary = FOREACH StatusGroup GENERATE group.returnflag AS returnflag, group.linestatus AS linestatus, SUM(SubLine.quantity) AS sum_qty, SUM(SubLine.extendedprice) AS sum_base_price, SUM(SubLine.disc_price) as sum_disc_price, SUM(SubLine.charge) as sum_charge, AVG(SubLine.quantity) as avg_qty, AVG(SubLine.extendedprice) as avg_price, AVG(SubLine.discount) as avg_disc, COUNT(SubLine) as count_order;
SortedSummary = ORDER PriceSummary BY returnflag, linestatus;
STORE SortedSummary INTO '$output/Q1out';

