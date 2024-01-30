create or replace view insurance_dw.prem_6 as
select
    *,
    cast(dx_d/(power(1+interest_rate, age+1))as decimal(38,7)) as cx
from insurance_dw.prem_5;