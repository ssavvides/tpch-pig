/*
SELECT P_BRAND, P_TYPE, P_SIZE, COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
FROM PARTSUPP, PART
WHERE P_PARTKEY = PS_PARTKEY AND P_BRAND <> 'Brand#45' AND P_TYPE NOT LIKE 'MEDIUM POLISHED%%'
AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9) AND PS_SUPPKEY NOT IN (SELECT S_SUPPKEY FROM SUPPLIER
 WHERE S_COMMENT LIKE '%%Customer%%Complaints%%')
GROUP BY P_BRAND, P_TYPE, P_SIZE
ORDER BY SUPPLIER_CNT DESC, P_BRAND, P_TYPE, P_SIZE
*/

SET default_parallel $reducers;

parts = LOAD '$input/part' USING PigStorage('|') AS (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment);

partsupp = LOAD '$input/partsupp' USING PigStorage('|') AS (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost:double, ps_comment);

supplier = LOAD '$input/supplier' USING PigStorage('|') AS (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment);

fsupplier = FILTER supplier BY (NOT s_comment MATCHES '.*Customer.*Complaints.*');
fs1 = FOREACH fsupplier GENERATE s_suppkey;

pss = JOIN partsupp BY ps_suppkey, fs1 BY s_suppkey;

fpartsupp = FOREACH pss GENERATE partsupp::ps_partkey as ps_partkey, partsupp::ps_suppkey as ps_suppkey;

fparts = FILTER parts BY 
(p_brand != 'Brand#45' AND 
 NOT (p_type MATCHES 'MEDIUM POLISHED.*') AND 
 p_size MATCHES '49|14|23|45|19|3|36|9');

pparts = FOREACH fparts GENERATE p_partkey, p_brand, p_type, p_size;

p1 = JOIN pparts BY p_partkey, fpartsupp by ps_partkey;
grResult = GROUP p1 BY (p_brand, p_type, p_size);
countResult = FOREACH grResult
{
  dkeys = DISTINCT p1.ps_suppkey;
  GENERATE group.p_brand as p_brand, group.p_type as p_type, group.p_size as p_size, COUNT(dkeys) as supplier_cnt;
}
orderResult = ORDER countResult BY supplier_cnt DESC, p_brand, p_type, p_size;

store orderResult into '$output/Q16out' USING PigStorage('|');
