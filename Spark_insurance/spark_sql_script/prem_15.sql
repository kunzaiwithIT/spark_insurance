create or replace view insurance_dw.prem_15 as
select
    *,
    cast(ci_cx*power(1+(select Interst_Rate_CV from insurance_dw.input),0.5) as decimal(17,7)) as ci_cx_,
    cast(lx/power(1+(select Interst_Rate_CV from insurance_dw.input),age)as decimal(17,7)) as dx,
    cast(lx_d/power(1+(select Interst_Rate_CV from insurance_dw.input),age) as decimal(17,7))as dx_d_
from insurance_dw.prem_14;