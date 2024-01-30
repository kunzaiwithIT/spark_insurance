create or replace view insurance_dw.prem_16 as
select
    *,
    cast(sum(dx*prem_15.db2_factor) over(partition by age_buy,ppp,sex rows between current row and unbounded following)/prem_15.dx as decimal(17,4)) as db2,
    cast(sum(dx*ppp_) over(partition by age_buy,ppp,sex rows between 1 following and unbounded following)/dx*power(1+(select Interst_Rate_CV from insurance_dw.input),0.5)as decimal(17,4)) as db5
from insurance_dw.prem_15;