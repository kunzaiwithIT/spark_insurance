create or replace view insurance_dw.prem_7 as
select
    *,
    cast(cx*power(1+interest_rate, 0.5) as decimal(38,7)) as cx_,
    cast(dx_ci/power(1+interest_rate, age+1) as decimal(38,7)) as ci_cx
from insurance_dw.prem_6;