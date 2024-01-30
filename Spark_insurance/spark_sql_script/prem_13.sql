create or replace view insurance_dw.prem_13 as
select age,sex,ppp,age_buy,
    kx,
    qx_d,
    qx_ci,
    dx_ci,
    lx,
    lx_d,
    cast(dx_d/power(1+(select Interst_Rate_CV from insurance_dw.input), age+1) as decimal(17,7)) as  cx,
    ppp_,
    bpp_,
    expense,
    db1,
    db2_factor,
    db3,
    db4
from insurance_dw.prem_src;
select * from insurance_dw.prem_src where policy_year=1;

create or replace view insurance_dw.prem_13 as
select
    *
from insurance_dw.prem_src
union
select
    null as age_buy,
    null as Nursing_Age,
    null as sex,
    null as T_Age,
    null as ppp,
    null as BPP,
    null as interest_rate,
    null as sa,
    0 as policy_year,
    null as age,
    null as ppp_,
    null as bpp_,
    null as qx,
    null as kx,
    null as qx_ci,
    null as qx_d    ,
    null as lx      ,
    null as lx_d    ,
    null as dx_d    ,
    null as dx_ci   ,
    cast(dx_d/power(1+(select Interst_Rate_CV from insurance_dw.input), age+1) as decimal(17,7)) as  cx,
    null as cx_     ,
    null as ci_cx   ,
    null as ci_cx_  ,
    null as dx      ,
    null as dx_d_   ,
    null as expense,
    null as db1           ,
    null as db2_factor    ,
    null as db2           ,
    null as db3           ,
    null as db4 ,
    null as db5
from insurance_dw.prem_src;
