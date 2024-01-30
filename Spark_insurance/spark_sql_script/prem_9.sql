create or replace view insurance_dw.prem_9 as
select
    t1.*,
    case when policy_year=1 then concat(round(r1*ppp_*100,2),'%')
         when policy_year=2 then concat(round(r2*ppp_*100,2),'%')
         when policy_year=3 then concat(round(r3*ppp_*100,2),'%')
         when policy_year=4 then concat(round(r4*ppp_*100,2),'%')
         when policy_year=5 then concat(round(r5*ppp_*100,2),'%')
         when policy_year>=6 then concat(round(r6_*ppp_*100,2),'%')
    end as expense,
    cast(bpp_*round((select Disability_Ratio from insurance_dw.input),2) as decimal(10,2)) as db1,
    cast(if(t1.age<t1.Nursing_Age,1,0)*(select input.Nursing_Ratio from insurance_dw.input)as decimal(10,2)) as db2_factor
from insurance_dw.prem_8 t1
join insurance_ods.pre_add_exp_ratio t2 on t1.ppp=t2.PPP;