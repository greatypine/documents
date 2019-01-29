create table o_waihu_user_list_2019-129_result as
select a.customer_id,a.storeno,ts.name as storename,
ifnull(a.name,'') as name,
a.idcard,
ifnull(ifnull(a.idcardsex,a.sex),'') as sex,
a.customer_phone,
ifnull(ifnull(idcardbirthday,birthday),'') as birthday,
case when is_associator != 0 then '是' else '' end as is_associator,
ifnull(addr_address,'') as addr_address,
a.last_order_time as last_order_time,
a.first_order_time as first_order_time,
a.associator_expiry_date,
a.sum_payable_price,
b.level,
c.payable_price_2018_top_20,
c.tag_2018_2_top20,
d.order_count_gt3,
d.tag_order_count_gt3,
e.jz_count,
f.gdxh_count
from (
select customer_id,customer_phone,storeno,
case when is_associator is null then 0 else 1 end as is_associator,
t.idcardsex,
t.sex,
t. name,
case when birthday is not null then to_date(birthday) else birthday end as birthday,
t.idcardbirthday,
t.associator_expiry_date,
t.addr_address,
t. idcard,
t.last_order_time,
t.first_order_time,
t.sum_payable_price
from (
select 
dm.customer_id,
dm.customer_phone,
dm.store_code as storeno,
case when tc.sex = 'male' then '男' when tc.sex = 'female' then '女' else null end as sex,
case when bc.idcard is not null and bc.idcard != '' and cast(substr(bc.idcard,17,1) as int)%2 =0 then '女' 
    when bc.idcard is not null and bc.idcard != '' and cast(substr(bc.idcard,17,1) as int)%2 != 0 then '男' else null end as idcardsex,
case when bc.idcard is not null and bc.idcard != '' then concat(substr(bc.idcard,7,4),'-',substr(bc.idcard,11,2),'-',substr(bc.idcard,13,2)) else bc.birthday end 
as idcardbirthday,
tc.name,
tc.birthday,
bc.customer_id as is_associator,
tc.associator_expiry_date,
dm.addr_address,
bci.idcard,
dm.first_order_time,
dm.last_order_time,
dm.sum_payable_price from (
--基本信息
select store_code,customer_id,min(customer_mobile_phone) as customer_phone,min(create_time) as first_order_time,max(create_time) as last_order_time,max(addr_address) as addr_address,sum(payable_price) as sum_payable_price from daqweb.df_mass_order_total where store_code in ('0010Y0057','0010Y0033','0010Y0203','0010Y0014','0022Y0019') group by 
store_code,customer_id
) dm inner join gemini.t_customer tc on (tc.id = dm.customer_id)
left join b_customer_associator bc on (tc.id = bc.customer_id)
left join b_customer_idcard bci on (bci.customer_id = tc.id)
) t 
) a inner join daqweb.t_store ts on (a.storeno = ts.storeno)
left join (
--消费品类明细
select customer_id,store_code, group_concat(level2_name,',') as level from (
select customer_id,store_code,total.level2_name from b_item_pro_total total where store_code in ('0010Y0057','0010Y0033','0010Y0203','0010Y0014','0022Y0019')
and level2_name is not null group by total.customer_id,total.store_code,total.level2_name
) t group by t.customer_id,t.store_code
) b on (a.customer_id = b.customer_id and a.storeno = b.store_code)
left join (
--2018年春节前后消费金额前20%的客户
    select customer_id,t.store_code,payable_price as payable_price_2018_top_20,'1' as tag_2018_2_top20  from (
        select t1.customer_id,t1.store_code,t1.payable_price,row_number() over(partition by store_code order by payable_price desc) as rn from (
        select customer_id,store_code,sum(payable_price) as payable_price from daqweb.df_mass_order_total where store_code in ('0010Y0057','0010Y0033','0010Y0203','0010Y0014','0022Y0019')
        and create_time >= '2018-02-01' and create_time <= '2018-02-28' group by customer_id,store_code
        ) t1 
    ) t inner join (
        select count(customer_id) as count,store_code from daqweb.df_mass_order_total where store_code in ('0010Y0057','0010Y0033','0010Y0203','0010Y0014','0022Y0019')
        and create_time >= '2018-02-01' and create_time <= '2018-02-28' group by store_code
    ) t2 on (t.store_code = t2.store_code) where rn/count <=0.2 
) c on (a.customer_id = c.customer_id and a.storeno = c.store_code)
left join (
-- 近30（2018-12-29）天消费次数大于3的用户
    select customer_id,store_code,count(1) as order_count_gt3,'1' as tag_order_count_gt3 from daqweb.df_mass_order_total where store_code in ('0010Y0057','0010Y0033','0010Y0203','0010Y0014','0022Y0019')
    and create_time > '2018-12-29' group by customer_id,store_code having count(1) >3
) d on (a.customer_id = d.customer_id and a.storeno = d.store_code)
left join (
--近半年家政次数（2018-07-01）
    select customer_id,store_code,count(1) as jz_count from b_item_pro_total total where store_code in ('0010Y0057','0010Y0033','0010Y0203','0010Y0014','0022Y0019')
    and (total.manager_category_names like '%家政%' or total.channel_name like '%家政%') and total.create_time >='2018-07-01' group by total.customer_id,total.store_code
) e on (a.customer_id = e.customer_id and a.storeno = e.store_code)
left join (
--近半高端洗护次数（2018-07-01）
    select customer_id,store_code,count(1) as gdxh_count from b_item_pro_total total where store_code in ('0010Y0057','0010Y0033','0010Y0203','0010Y0014','0022Y0019')
    and (total.manager_category_names like '%高端洗护%' or total.channel_name like '%高端洗护%') and total.create_time >='2018-07-01' group by total.customer_id,total.store_code
) f on (a.customer_id = f.customer_id and a.storeno = f.store_code);