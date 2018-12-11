#建表语句
drop table if exists gabase.b_user_profile;
create table b_user_profile(
	customer_id string,
	customer_name string,
	customer_phone string,
	idcardmark string,
	birthday string,
	sex string,
	regist_time string,
	associator_level int,
	customer_source string,
	trading_price_month_avg double,
	order_sn_max string,
	trading_price_max double,
	last_order_sn string,
	last_order_time string,
	last_order_item_count int,
	last_order_trading_price double,
	last_order_store_id string,
	last_order_storeno string,
	last_order_eshop_id string,
	last_order_city_code string,
	last_order_channel_name string,
	last_order_area_code string,
	trading_price_sum double,
	order_count int,
	item_count int,
	store_count int,
	eshop_count int,
	city_count int,
	channel_count int,
	area_count int,
	real_score int,
	ga_flag int,
	ylc_create_time string,
	create_time string,
	update_time string
);

#清空表内数据
TRUNCATE b_user_profile;

#用户档案表清洗历史数据
#第一步 插入国安平台数据
insert into gabase.b_user_profile
select 
vcc.customer_id as customer_id,
tc.name as customer_name,
tc.mobilephone as customer_phone,
case when th.cardid is not null and th.cardid != '' then th.cardid when ylc.usercard is not null and ylc.usercard != '' then ylc.usercard else null end  as idcardmark,
tc.birthday as birthday,
tc.sex as sex,
tc.create_time as regist_time,
tc.associator_level as associator_level,
tc.customer_source as customer_source,
case when vcc.month_cus < 1 then vcc.trading_price_sum else dround(vcc.trading_price_sum/vcc.month_cus,2) end as trading_price_month_avg,
t_b.order_sn as order_sn_max,
vcc.trading_price_max as trading_price_max,
tor2.order_sn as last_order_sn,
vcc.last_order_time as last_order_time,
tor2.total_quantity as last_order_item_count,
tor2.trading_price as last_order_trading_price,
tor2.store_id as last_order_store_id,
tor2.store_code as last_order_storeno,
tor2.eshop_id as last_order_eshop_id,
tor2.store_city_area_code as last_order_city_code,
tor2.dcc_name as last_order_channel_name,
null as last_order_area_code,
vcc.trading_price_sum,
cast(vcc.order_count as int) as order_count,
cast(vcc.item_count as int) as item_count,
cast(vcs.store_count as int) as store_count,
cast(vce.eshop_count as int) as eshop_count,
cast(vccc.city_count as int) as city_count,
cast(vch.channel_count as int) as channel_count,
null as area_count,
null as real_score,
1 as ga_flag,
case when ylc.createtime is not null and ylc.createtime != '' then ylc.createtime else null end as ylc_create_time,
now() as create_time,
now() as update_time
from v_customer_conclusion vcc 
LEFT JOIN v_customer_city_count vccc ON (vcc.customer_id = vccc.customer_id)
LEFT JOIN v_customer_channel_count vch ON (vcc.customer_id = vch.customer_id)
LEFT JOIN v_customer_eshop_count vce ON (vcc.customer_id = vce.customer_id)
LEFT JOIN v_customer_store_count vcs ON (vcc.customer_id = vcs.customer_id)
LEFT JOIN 
(
	select b.customer_id as customer_id,max(tor1.order_sn) as order_sn,max(b.trading_price) as trading_price from 
	(select max(tor.trading_price) as trading_price,customer_id  from gemini.t_order tor join gemini.t_eshop te on (tor.eshop_id = te.id) GROUP BY customer_id) b
	join gemini.t_order tor1 
	ON (tor1.customer_id = b.customer_id and tor1.trading_price = b.trading_price) group by b.customer_id
) t_b ON (t_b.customer_id = vcc.customer_id) 
LEFT JOIN (
    select * from (
    select *,ROW_NUMBER() OVER (partition BY order_sn ORDER BY order_create_time DESC) as rn from gabase.v_full_success_order v  
    ) t where  t.rn = 1
) tor2 on (tor2.order_sn = vcc.last_order_sn)
LEFT JOIN 
	gemini.t_customer tc on (vcc.customer_id = tc.id)
LEFT JOIN (
select * from (
    select *,ROW_NUMBER() OVER (partition BY phone ORDER BY updatetime DESC) as rn from daqweb.t_sync_record v  
    ) t where  t.rn = 1
) th ON (th.phone = tc.mobilephone and th.phone is not null and th.phone != '')
LEFT JOIN gabase.v_yanglc_user ylc ON (ylc.userphone = tc.mobilephone);

#第二部 插入养老餐数据





#养老餐视图
drop view if exists gabase.v_yanglc_user;
create view gabase.v_yanglc_user as
select
	ylu.id as userid,
	ylu. name as username,
	ylu.tel as userphone,
	ylu.IDcard as usercard, 
	ylu.create_time as createtime,
 	ylu.store_id as storeid,
 	ylc.city_code as citycode,
 	ylc. name as storename,
 	ylc.province_code as provincecode
from
	gabase.c_customer ylu,gabase.c_store ylc
where ylu.IDcard not in (select IDcard from gabase.c_customer where substring(IDcard, 7, 2) > '18' and substring(IDcard, 7, 2) < '21' group by IDcard HAVING count(IDcard) > 1)
and ylu.tel not in (select tel from gabase.c_customer where substring(IDcard, 7, 2) > '18' and substring(IDcard, 7, 2) < '21' group by tel HAVING count(tel) > 1)
and substring(ylu.IDcard, 7, 2) > '18' and substring(ylu.IDcard, 7, 2) < '21' and length(ylu.tel)<30 and ylu.store_id = ylc.id;

#创建总结性数据视图：共5个
#1
drop view if exists gabase.v_customer_conclusion;
create view gabase.v_customer_conclusion as
select 
    tor.customer_id,
    max(tor.trading_price) as trading_price_max,
    months_between(max(tor.create_time),min(tor.create_time)) as month_cus,
    min(tor.create_time) as first_order_time,
    sum(tor.trading_price) as trading_price_sum,
    sum(tor.total_quantity) as item_count,
    count(1) as order_count
from gemini.t_order tor
join gemini.t_eshop te on (tor.eshop_id = te.id)
where
tor.sign_time < concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') and 
te.`name` NOT LIKE '%测试%' AND te.white!='QA' AND sign_time is not null
group by tor.customer_id;

#2
drop view if exists gabase.v_customer_store_count;
create view gabase.v_customer_store_count as
select 
    tor.customer_id,
    count(distinct tor.store_id) as store_count
from gemini.t_order tor
join gemini.t_eshop te on (tor.eshop_id = te.id)
LEFT JOIN gemini.t_store ts ON (ts.id = ifnull(tor.normal_store_id, tor.store_id))
where
tor.sign_time < concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') and 
te.`name` NOT LIKE '%测试%' AND te.white!='QA' AND sign_time is not null
group by tor.customer_id;

#3
drop view if exists gabase.v_customer_eshop_count;
create view gabase.v_customer_eshop_count as
select 
    tor.customer_id,
    count(distinct tor.eshop_id) as eshop_count
from gemini.t_order tor
join gemini.t_eshop te on (tor.eshop_id = te.id)
LEFT JOIN gemini.t_store ts ON (ts.id = tor.store_id)
LEFT JOIN gemini.t_department_channel tdc ON (te.channel_id = tdc.id)
where
tor.sign_time < concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') and 
te.`name` NOT LIKE '%测试%' AND te.white!='QA' AND sign_time is not null
group by tor.customer_id;

#4
drop view if exists gabase.v_customer_city_count;
create view gabase.v_customer_city_count as
select 
    tor.customer_id,
    count(distinct ts.city_code) as city_count
from gemini.t_order tor
join gemini.t_eshop te on (tor.eshop_id = te.id)
LEFT JOIN gemini.t_store ts ON (ts.id = tor.store_id)
LEFT JOIN gemini.t_department_channel tdc ON (te.channel_id = tdc.id)
where
tor.sign_time < concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') and 
te.`name` NOT LIKE '%测试%' AND te.white!='QA' AND sign_time is not null
group by tor.customer_id;

#5
drop view if exists gabase.v_customer_channel_count;
create view gabase.v_customer_channel_count as
select 
    tor.customer_id,
    count(distinct tdc.id) as channel_count
from gemini.t_order tor
join gemini.t_eshop te on (tor.eshop_id = te.id)
LEFT JOIN gemini.t_store ts ON (ts.id = tor.store_id)
LEFT JOIN gemini.t_department_channel tdc ON (te.channel_id = tdc.id)
where
tor.sign_time < concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') and 
te.`name` NOT LIKE '%测试%' AND te.white!='QA' AND sign_time is not null
group by tor.customer_id;


#当天数据查询
SELECT t_base.customer_id,
       tc.name AS customer_name,
       tc.mobilephone AS customer_phone,
       CASE
           WHEN th.cardid IS NOT NULL
                AND th.cardid != '' THEN th.cardid
           WHEN ylc.usercard IS NOT NULL
                AND ylc.usercard != '' THEN ylc.usercard
           ELSE NULL
       END AS idcardmark,
       tc.birthday AS birthday,
       tc.sex AS sex,
       tc.create_time AS regist_time,
       tc.associator_level AS associator_level,
       tc.customer_source AS customer_source,
       CASE
           WHEN vcc.month_cus < 1 THEN vcc.trading_price_sum
           ELSE dround(vcc.trading_price_sum/vcc.month_cus,2)
       END AS trading_price_month_avg,
       t_b.order_sn AS order_sn_max,
       vcc.trading_price_max AS trading_price_max,
       tor2.order_sn AS last_order_sn,
       vcc.last_order_time AS last_order_time,
       tor2.total_quantity AS last_order_item_count,
       tor2.trading_price AS last_order_trading_price,
       tor2.store_id AS last_order_store_id,
       tor2.store_code AS last_order_storeno,
       tor2.eshop_id AS last_order_eshop_id,
       tor2.store_city_area_code AS last_order_city_code,
       tor2.dcc_name AS last_order_channel_name,
       NULL AS last_order_area_code,
       vcc.trading_price_sum,
       cast(vcc.order_count AS int) AS order_count,
       cast(vcc.item_count AS int) AS item_count,
       cast(vcs.store_count AS int) AS store_count,
       cast(vce.eshop_count AS int) AS eshop_count,
       cast(vccc.city_count AS int) AS city_count,
       cast(vch.channel_count AS int) AS channel_count,
       NULL AS area_count,
       NULL AS real_score,
       1 AS ga_flag,
       CASE
           WHEN ylc.createtime IS NOT NULL
                AND ylc.createtime != '' THEN ylc.createtime
           ELSE NULL
       END AS ylc_create_time,
       NULL AS create_time,
       NULL AS update_time
FROM
  ( SELECT tor.customer_id
   FROM gemini.t_order tor
   JOIN gemini.t_eshop te ON (tor.eshop_id = te.id)
   WHERE tor.sign_time >= concat(from_unixtime(unix_timestamp(date_sub(now(), 1)),"yyyy-MM-dd"), ' 00:00:00')
     AND tor.sign_time < concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')
     AND te.`name` NOT LIKE '%测试%'
     AND te.white!='QA'
     AND sign_time IS NOT NULL
   GROUP BY tor.customer_id) t_base
LEFT JOIN v_customer_conclusion vcc ON (t_base.customer_id = vcc.customer_id)
LEFT JOIN v_customer_city_count vccc ON (t_base.customer_id = vccc.customer_id)
LEFT JOIN v_customer_channel_count vch ON (t_base.customer_id = vch.customer_id)
LEFT JOIN v_customer_eshop_count vce ON (t_base.customer_id = vce.customer_id)
LEFT JOIN v_customer_store_count vcs ON (t_base.customer_id = vcs.customer_id)
LEFT JOIN
  ( SELECT b.customer_id AS customer_id,
           max(tor1.order_sn) AS order_sn,
           max(b.trading_price) AS trading_price
   FROM
     (SELECT max(tor.trading_price) AS trading_price,
             customer_id
      FROM gemini.t_order tor
      JOIN gemini.t_eshop te ON (tor.eshop_id = te.id)
      GROUP BY customer_id) b
   JOIN gemini.t_order tor1 ON (tor1.customer_id = b.customer_id
                                AND tor1.trading_price = b.trading_price)
   GROUP BY b.customer_id) t_b ON (t_b.customer_id = vcc.customer_id)
LEFT JOIN
  ( SELECT *
   FROM
     ( SELECT *,
              ROW_NUMBER() OVER (partition BY order_sn
                                 ORDER BY order_create_time ASC) AS rn
      FROM gabase.v_full_success_order v) t
   WHERE t.rn = 1 ) tor2 ON (tor2.order_sn = vcc.last_order_sn)
LEFT JOIN gemini.t_customer tc ON (vcc.customer_id = tc.id)
LEFT JOIN
  (SELECT *
   FROM
     ( SELECT *,
              ROW_NUMBER() OVER (partition BY phone
                                 ORDER BY updatetime DESC) AS rn
      FROM daqweb.t_sync_record v) t
   WHERE t.rn = 1 ) th ON (th.phone = tc.mobilephone
                           AND th.phone IS NOT NULL
                           AND th.phone != '')
LEFT JOIN gabase.v_yanglc_user ylc ON (ylc.userphone = tc.mobilephone);











#缺少的mongo里面的表
tiny_dispatch mongo 
t_customer_info_record_ext  inviteCode mongo



select info_village_code,area_code,info_employee_a_no from df_mass_order_monthly where order_sn =#{order_sn} limit 1 





select 
	tor.customer_id,
	sum(tor.trading_price) as trading_price_sum,
	day(now())
	from gemini.t_order tor
	join gemini.t_eshop te on (tor.eshop_id = te.id)
	where
	tor.sign_time > concat(from_unixtime(unix_timestamp(months_sub(date_sub(now(),  dayofmonth(now())-1), 0)),"yyyy-MM-dd"), ' 00:00:00') 
	and tor.sign_time < concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')  and 
	te.`name` NOT LIKE '%测试%' AND te.white!='QA' AND sign_time is not null
	group by tor.customer_id



select day(now());








create view gabase.v_customer_conclusion as
select 
    tor.customer_id,
    max(tor.create_time) as last_order_time,
    max(tor.order_sn) as last_order_sn,
    count(1) as order_count
from gemini.t_order tor
join gemini.t_eshop te on (tor.eshop_id = te.id)
LEFT JOIN gemini.t_store ts ON (ts.id = tor.store_id)
LEFT JOIN gemini.t_department_channel tdc ON (te.channel_id = tdc.id)
where
tor.sign_time < concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') and 
te.`name` NOT LIKE '%测试%' AND te.white!='QA' AND sign_time is not null
group by tor.customer_id;