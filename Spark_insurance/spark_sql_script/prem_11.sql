create table insurance_dw.prem_std as
select
    distinct *,
    (select sa from insurance_dw.input)*(T11+V11+W11)/(Q11-T9-S11-X11-Y11) as prem
from(
select
    *,
    cast(sum(case when policy_year=1 then 0.5*ci_cx_*db1*power(1+interest_rate,-0.25)
             when policy_year>=2 then ci_cx_*db1 end) over (partition by age_buy,ppp,sex order by policy_year rows between
                 unbounded preceding and unbounded following) as decimal(38,7)) as T11,
    cast(sum(case when policy_year=1 then 0.5*ci_cx_*db2*power(1+interest_rate,-0.25)
            when policy_year>=2 then ci_cx_*db2 end) over (partition by age_buy,ppp,sex order by policy_year rows between
                 unbounded preceding and unbounded following)as decimal(38,7)) as V11,
    cast(sum(dx*db3) over (partition by age_buy,ppp,sex order by policy_year rows between
                 unbounded preceding and unbounded following) as decimal(38,7)) as W11,
    cast(sum(dx*ppp_) over (partition by age_buy,ppp,sex order by policy_year rows between
                 unbounded preceding and unbounded following) as decimal(38,4)) as Q11,
    cast(0.5*first(ci_cx_) over (partition by age_buy,ppp,sex order by policy_year)*power(1+interest_rate,0.25) as decimal(38,7)) as T9,
    cast(0.5*first(ci_cx_) over (partition by age_buy,ppp,sex order by policy_year)*power(1+interest_rate,0.25) as decimal(38,7)) as V9,
    cast(sum(dx*cast(split(expense,'%')[0] as decimal(38,6))/100) over (partition by age_buy,ppp,sex order by policy_year rows between
                 unbounded preceding and unbounded following) as decimal(38,5)) as S11,
    cast(sum(cx_*db4) over (partition by age_buy,ppp,sex order by policy_year rows between
                 unbounded preceding and unbounded following) as decimal(38,7)) as X11,
    cast(sum(ci_cx_*db5) over (partition by age_buy,ppp,sex order by policy_year rows between
                 unbounded preceding and unbounded following) as decimal(38,5)) as Y11
from insurance_dw.prem_src);






