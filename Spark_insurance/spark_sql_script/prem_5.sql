set spark.sql.decimalOperations.allowPrecisionLoss = false;
-- shuffle分区数
set spark.sql.shuffle.partitions = 4;
create table if not exists insurance_dw.prem_5 as
select
    *,
    cast(lx_d*qx_d as decimal(38,7)) as dx_d,
    cast(lx_d*qx_ci as decimal(38,7)) as dx_ci
from (
        select
            *,
            pandas_func(qx_d,qx_ci) over(partition by age_buy,ppp,sex order by policy_year) as lx_d
        from insurance_dw.prem_4
    );

