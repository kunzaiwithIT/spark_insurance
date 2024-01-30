create or replace view insurance_dw.prem_8 as
select
    *,
    cast(ci_cx*power(1+interest_rate, 0.5) as decimal(38,7)) as ci_cx_,
    cast(lx/power(1+interest_rate, age) as decimal(38,7)) as dx,
    cast(lx_d/power(1+interest_rate, age) as decimal(38,7)) as dx_d_
from insurance_dw.prem_7;