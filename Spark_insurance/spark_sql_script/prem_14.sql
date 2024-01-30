create or replace view insurance_dw.prem_14 as
select
    *,
    cast(cx*power(1+(select Interst_Rate_CV from insurance_dw.input),0.5) as decimal(17,7)) as cx_,
    cast(dx_ci/power(1+(select Interst_Rate_CV from insurance_dw.input),1+age) as decimal(17,7)) as ci_cx
from insurance_dw.prem_13;