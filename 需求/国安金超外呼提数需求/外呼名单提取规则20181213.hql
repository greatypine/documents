drop table if exists o_waihu_user_list_2018;
-- 1. 最近90天下单数量>3的社员用户
create table o_waihu_user_list_2018 as 
select t.*, cm.mobilephone from 
(select b.customer_id, b.value, '社员-最近90天下单数量大于3' as source from b_user_tag b where `key`='cum_orders_90ds' and cast(value as double) > 3    --90天内下单数量大于3
    and customer_id in (select distinct customer_id from b_user_tag  where `key`='associator_199' and value='2'   -- 社员
    )) t inner join 
(select customer_id, value as mobilephone from b_user_tag where key='mobilephone' 
  and customer_id not in (select distinct customer_id from b_user_tag where key='guoan_employee')) cm    -- 非国安线上/线下员工
  on t.customer_id=cm.customer_id;

-- 2.最近90天消费金额前20%的社员用户
insert into o_waihu_user_list_2018 
select t.*, cm.mobilephone from 
(select b.customer_id, b.value, '社员-最近90天消费金额前20%' as source from b_user_tag b where `key`='cum_buy_90ds' 
    and value is not null
    and customer_id in (select distinct customer_id from b_user_tag  where `key`='associator_199' and value='2'   -- 社员
    )
) t inner join 
(select customer_id, value as mobilephone from b_user_tag where key='mobilephone' 
  and customer_id not in (select distinct customer_id from b_user_tag where key='guoan_employee')) cm    -- 非国安线上/线下员工
  on t.customer_id=cm.customer_id
  where value is not null
order by cast(value as double) desc limit 22000;

-- 3.最近90天累计消费金额大于5万的社员用户
insert into o_waihu_user_list_2018 
select t.*, cm.mobilephone from 
(select b.customer_id, b.value, '社员-最近90天累计消费金额大于5万' as source from b_user_tag b where `key`='cum_buy_90ds' and cast(value as double)>50000
    and customer_id in (select distinct customer_id from b_user_tag  where `key`='associator_199' and value='2'   -- 社员
    )) t inner join 
(select customer_id, value as mobilephone from b_user_tag where key='mobilephone' 
  and customer_id not in (select distinct customer_id from b_user_tag where key='guoan_employee')) cm    -- 非国安线上/线下员工
  on t.customer_id=cm.customer_id
  where value is not null;
  
-- 4.最近360天累计消费金额大于5万的社员用户
insert into o_waihu_user_list_2018 
select t.*, cm.mobilephone from 
(select b.customer_id, b.value, '社员-最近360天累计消费金额大于5万' as source from b_user_tag b where `key`='cum_buy_360ds' and cast(value as double)>50000
    and customer_id in (select distinct customer_id from b_user_tag  where `key`='associator_199' and value='2'   -- 社员
    )) t inner join 
(select customer_id, value as mobilephone from b_user_tag where key='mobilephone' 
  and customer_id not in (select distinct customer_id from b_user_tag where key='guoan_employee')) cm    -- 非国安线上/线下员工
  on t.customer_id=cm.customer_id
  where value is not null;

--5. 最近90天下单次数大于5的非社员用户
insert into o_waihu_user_list_2018 
select t.*, cm.mobilephone from 
(select b.customer_id, b.value, '非社员-最近90天下单数量大于5' as source from b_user_tag b where `key`='cum_orders_90ds' and cast(value as double) > 5    --90天内下单数量大于5
    and customer_id in (select distinct customer_id from b_user_tag  where `key`='associator_199' and value!='2'   -- 非社员
    )) t inner join 
(select customer_id, value as mobilephone from b_user_tag where key='mobilephone' 
  and customer_id not in (select distinct customer_id from b_user_tag where key='guoan_employee')) cm    -- 非国安线上/线下员工
  on t.customer_id=cm.customer_id;
  
  --6. 最近90天下单次数大于2的非社员用户
insert into o_waihu_user_list_2018 
select t.*, cm.mobilephone from 
(select b.customer_id, b.value, '非社员-最近90天下单数量大于2' as source from b_user_tag b where `key`='cum_orders_90ds' and cast(value as double) > 2    --90天内下单数量大于2
    and customer_id in (select distinct customer_id from b_user_tag  where `key`='associator_199' and value!='2'   -- 非社员
    )) t inner join 
(select customer_id, value as mobilephone from b_user_tag where key='mobilephone' 
  and customer_id not in (select distinct customer_id from b_user_tag where key='guoan_employee')) cm    -- 非国安线上/线下员工
  on t.customer_id=cm.customer_id;
  
  --7. 最近360天内下单次数大于5的非社员用户
  insert into o_waihu_user_list_2018 
select t.*, cm.mobilephone from 
(select b.customer_id, b.value, '非社员-最近360天下单数量大于5' as source from b_user_tag b where `key`='cum_orders_360ds' and cast(value as double) > 5    --近360天内下单数量大于2
    and customer_id in (select distinct customer_id from b_user_tag  where `key`='associator_199' and value!='2'   -- 非社员
    )) t inner join 
(select customer_id, value as mobilephone from b_user_tag where key='mobilephone' 
  and customer_id not in (select distinct customer_id from b_user_tag where key='guoan_employee')) cm    -- 非国安线上/线下员工
  on t.customer_id=cm.customer_id;
 
 -- 8.累计消费金额大于5万的非社员用户
insert into o_waihu_user_list_2018 
select t.*, cm.mobilephone from 
(select b.customer_id, b.value, '非社员-累计消费金额大于5万' as source from b_user_tag b where `key`='cum_buy_tot' and cast(value as double)>50000
    and customer_id in (select distinct customer_id from b_user_tag  where `key`='associator_199' and value!='2'   -- 社员
    )) t inner join 
(select customer_id, value as mobilephone from b_user_tag where key='mobilephone' 
  and customer_id not in (select distinct customer_id from b_user_tag where key='guoan_employee')) cm    -- 非国安线上/线下员工
  on t.customer_id=cm.customer_id
  where value is not null;
  
 -- 9.累计消费金额前20%的非社员用户(取前3万)
insert into o_waihu_user_list_2018 
select t.*, cm.mobilephone from 
(select b.customer_id, b.value, '非社员-累计消费金额前20%' as source from b_user_tag b where `key`='cum_buy_tot' and b.value is not null   -- 3010904
    and customer_id in (select distinct customer_id from b_user_tag  where `key`='associator_199' and value!='2'   -- 社员
    )) t inner join 
(select customer_id, value as mobilephone from b_user_tag where key='mobilephone' 
  and customer_id not in (select distinct customer_id from b_user_tag where key='guoan_employee')) cm    -- 非国安线上/线下员工
  on t.customer_id=cm.customer_id
  order by cast(value as double) desc limit 30000; 
  
 -- 10.最近90天消费金额前20%的非社员用户
insert into o_waihu_user_list_2018 
select t.*, cm.mobilephone from 
(select b.customer_id, b.value, '非社员-最近90天消费金额前20%' as source from b_user_tag b where `key`='cum_buy_90ds' and b.value is not null  --107976
    and customer_id in (select distinct customer_id from b_user_tag  where `key`='associator_199' and value!='2'   -- 社员
    )) t inner join 
(select customer_id, value as mobilephone from b_user_tag where key='mobilephone' 
  and customer_id not in (select distinct customer_id from b_user_tag where key='guoan_employee')) cm    -- 非国安线上/线下员工
  on t.customer_id=cm.customer_id
  order by cast(value as double) desc limit 21000; 
  
 --11.最近90天单笔消费金额超5万
 insert into o_waihu_user_list_2018 
select t.*, cm.mobilephone from 
(select b.customer_id, b.value, '最近90天单笔消费金额超5万' as source from b_user_tag b where `key`='max_90ds' and cast(value as double)>50000) t inner join 
(select customer_id, value as mobilephone from b_user_tag where key='mobilephone' 
  and customer_id not in (select distinct customer_id from b_user_tag where key='guoan_employee')) cm    -- 非国安线上/线下员工
  on t.customer_id=cm.customer_id
   where value is not null;
 
  --12.最近360天单笔消费金额超5万
 insert into o_waihu_user_list_2018 
select t.*, cm.mobilephone from 
(select b.customer_id, b.value, '最近360天单笔消费金额超5万' as source from b_user_tag b where `key`='max_360ds' and cast(value as double)>50000) t inner join 
(select customer_id, value as mobilephone from b_user_tag where key='mobilephone' 
  and customer_id not in (select distinct customer_id from b_user_tag where key='guoan_employee')) cm    -- 非国安线上/线下员工
  on t.customer_id=cm.customer_id
   where value is not null;
 
   --13. 最近180天购买过单价超500元酒水的客户
    insert into o_waihu_user_list_2018
select voi.customer_id, cast(sum(voi.payable_price) as string) as value, '最近180天购买过单价超500元酒水的客户' as source, voi.cust_mobilephone as mobilephone from gabase.v_full_success_order_item voi 
inner join gemini.t_product tp on voi.eshop_pro_id=tp.id and (content_tag like '%白酒%' or content_tag like '%酒水%') and content_price >= 500
inner join 
(select customer_id, value as mobilephone from b_user_tag where key='mobilephone' 
  and customer_id not in (select distinct customer_id from b_user_tag where key='guoan_employee')) cm    -- 非国安线上/线下员工
  on voi.customer_id=cm.customer_id
 where datediff(now(), voi.order_create_time) <= 180 
group by voi.customer_id, voi.cust_mobilephone;
  
   --14. 累计购买过单价超500元酒水的客户
    insert into o_waihu_user_list_2018
select voi.customer_id, cast(sum(voi.payable_price) as string) as value, '累计购买过单价超500元酒水的客户' as source, voi.cust_mobilephone as mobilephone from gabase.v_full_success_order_item voi 
inner join gemini.t_product tp on voi.eshop_pro_id=tp.id and (content_tag like '%白酒%' or content_tag like '%酒水%') and content_price >= 500
inner join 
(select customer_id, value as mobilephone from b_user_tag where key='mobilephone' 
  and customer_id not in (select distinct customer_id from b_user_tag where key='guoan_employee')) cm    -- 非国安线上/线下员工
  on voi.customer_id=cm.customer_id
group by voi.customer_id, voi.cust_mobilephone;



--Result: 提取结果数据
drop table if exists o_waihu_user_list_2018_result;
create table o_waihu_user_list_2018_result as
select waihu.mobilephone, tc.sex, tc.register_name, tc.in_city_name, group_concat(source, ',') as tag_list from gabase.o_waihu_user_list_2018 waihu
left join gabase.s_user_basic_info tc on tc.customer_id=waihu.customer_id
where waihu.mobilephone in (select distinct mobilephone from (
select mobilephone, count(source) cs from gabase.o_waihu_user_list_2018 group by mobilephone order by count(source) desc
) tmp where tmp.cs>1
) and length(waihu.mobilephone)=11 and strleft(waihu.mobilephone, 1)='1'
group by waihu.mobilephone, tc.sex, tc.register_name, tc.in_city_name;



--hive脚本提取数据
hive -e "select mobilephone, case when sex='femail' then '女' when sex='male' then '男' else '' end as sex, in_city_name, tag_list from gabase.o_waihu_user_list_2018_result order by mobilephone limit 30000" > o_waihu_user_list_2018_result.csv