create or replace view insurance_dw.prem_10 as
select
    *,
    cast(sum(dx*prem_9.db2_factor) over (partition by age_buy,ppp,sex order by policy_year rows between
        current row and unbounded following)/prem_9.dx as decimal(10,2)) as db2,
    cast(if(age>=Nursing_Age,1,0)*(select Nursing_Ratio from insurance_dw.input) as decimal(10,2)) as db3,
    if(ppp>=prem_9.policy_year,policy_year,ppp) as db4,
    cast(sum(dx*ppp_) over(partition by age_buy,ppp,sex order by policy_year rows between 1 following and unbounded following)
        /dx*power(1+interest_rate, 0.5) as decimal(10,4)) as db5
from insurance_dw.prem_9;

