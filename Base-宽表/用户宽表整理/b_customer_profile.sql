#用户档案表建表语句
create table b_customer_profile(
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
	last_order_store_no string,
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
	create_time string,
	update_time string,
) partitioned by (last_order_time string,create_time string)

#用户档案表清洗历史数据
select 
a.customer_id as customer_id,
tc.name as customer_name,
tc.mobilephone as customer_phone,
tc.sex as sex,
tc.birthday as birthday,
tc.create_time as regist_time,
tc.associator_level as associator_level,
tc.customer_source as customer_source,
t_1.order_sn as order_sn_max,
a.trading_price_max as trading_price_max,
tor2.order_sn as last_order_sn,
a.last_order_time as last_order_time,
tor2.total_quantity as last_order_item_count,
tor2.trading_price as last_order_trading_price,
tor2.store_id as last_order_store_id,
tor2.code as last_order_store_no,
tor2.eshop_id as last_order_eshop_id,
tor2.city_code as last_order_city_code,
tor2.channel_name as last_order_channel_name,
a.trading_price_sum,
a.order_count,
a.item_count,
a.store_count,
a.eshop_count,
a.city_count,
a.channel_count
from (
	select 
	tor.customer_id,
	max(tor.trading_price) as trading_price_max,
	max(tor.create_time) as last_order_time,
	max(tor.order_sn) as last_order_sn,
	sum(tor.trading_price) as trading_price_sum,
	count(1) as order_count,
	sum(tor.total_quantity) as item_count,
	ndv(tor.store_id) as store_count,
	ndv(tor.eshop_id) as eshop_count,
	ndv(ts.city_code) as city_count,
	ndv(tdc.id) as channel_count
	from gemini.t_order tor
	join gemini.t_eshop te on (tor.eshop_id = te.id)
	LEFT JOIN gemini.t_store ts ON (ts.id = tor.store_id)
	LEFT JOIN gemini.t_department_channel tdc ON (te.channel_id = tdc.id)
	where
	te.`name` NOT LIKE '%测试%' AND te.white!='QA' AND sign_time is not null
	group by tor.customer_id
) a 
LEFT JOIN 
(
	select b.customer_id as customer_id,max(tor1.order_sn) as order_sn,max(b.trading_price) as trading_price from 
	(select max(tor.trading_price) as trading_price,customer_id  from gemini.t_order tor join gemini.t_eshop te on (tor.eshop_id = te.id) GROUP BY customer_id) b
	join gemini.t_order tor1 
	ON (tor1.customer_id = b.customer_id and tor1.trading_price = b.trading_price) group by b.customer_id
) t_1 ON (t_1.customer_id = a.customer_id) 
LEFT JOIN 
(	
	select tor.total_quantity,tor.trading_price,ts.code,tor.store_id,tor.eshop_id,tor.order_sn,ts.city_code,tdc.name as channel_name from gemini.t_order tor 
	join gemini.t_eshop te on (tor.eshop_id = te.id)
	LEFT JOIN gemini.t_store ts ON (ts.id = tor.store_id)
	LEFT JOIN gemini.t_department_channel tdc ON (te.channel_id = tdc.id)
) tor2 on (tor2.order_sn = a.last_order_sn)
LEFT JOIN 
	gemini.t_customer tc on (a.customer_id = tc.id);





#缺少的mongo里面的表
tiny_dispatch mongo 
t_customer_info_record_ext  inviteCode mongo



select info_village_code,area_code,info_employee_a_no from df_mass_order_monthly where order_sn =#{order_sn} limit 1 
