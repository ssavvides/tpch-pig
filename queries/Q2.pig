SET default_parallel $reducers;

Part = load '$input/part' USING PigStorage('|') as (p_partkey:long, p_name:chararray, p_mfgr:chararray, p_brand:chararray, p_type:chararray, p_size:long, p_container:chararray, p_retailprice:double, p_comment:chararray);

Supplier = load '$input/supplier' USING PigStorage('|') as (s_suppkey:long, s_name:chararray, s_address:chararray, s_nationkey:int, s_phone:chararray, s_acctbal:double, s_comment:chararray);

Partsupp = load '$input/partsupp' USING PigStorage('|') as (ps_partkey:long, ps_suppkey:long, ps_availqty:long, ps_supplycost:double, ps_comment:chararray);

Nation = load '$input/nation' USING PigStorage('|') as (n_nationkey:int, n_name:chararray, n_regionkey:int, n_comment:chararray);

Region = load '$input/region' USING PigStorage('|') as (r_regionkey:int, r_name:chararray, r_comment:chararray);

FRegion = filter Region by r_name == 'EUROPE';
FR_N = join FRegion BY r_regionkey, Nation BY n_regionkey;
FR_N_S = join FR_N BY n_nationkey, Supplier BY s_nationkey;
FR_N_S_PS = join FR_N_S BY s_suppkey, Partsupp BY ps_suppkey;

FPart = filter Part by p_size == 15 and p_type matches '.*BRASS';
FR_N_S_PS_FP = join FR_N_S_PS by ps_partkey, FPart by p_partkey;

G1 = group FR_N_S_PS_FP by ps_partkey;
Min = FOREACH G1 GENERATE flatten(FR_N_S_PS_FP), MIN(FR_N_S_PS_FP.ps_supplycost) as min_ps_supplycost;
MinCost = filter Min by ps_supplycost == min_ps_supplycost;

RawResults = foreach MinCost generate s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment;
SortedMinimumCostSupplier = ORDER RawResults BY s_acctbal DESC, n_name, s_name, p_partkey;

HundredMinimumCostSupplier = LIMIT SortedMinimumCostSupplier 100;

STORE HundredMinimumCostSupplier into '$output/Q2out' USING PigStorage('|');

