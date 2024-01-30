set spark.sql.decimalOperations.allowPrecisionLoss = false;
-- shuffle分区数
set spark.sql.shuffle.partitions = 4;
create table if not exists insurance_dw.prem_4 as
select
    *,
    pandas_func(qx) over(partition by age_buy,ppp,sex order by policy_year) as lx
from insurance_dw.prem_3;

