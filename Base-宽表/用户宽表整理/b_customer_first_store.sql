#用户首次消费门店建表语句
create table b_customer_first_store(
	customer_id string,
	customer_phone string,
	store_id string,
	storeno string,
	eshop_id string,
	cityno string,
	city_name string,
	area_code string,
	tiny_village_code string,
	employee_a_no string,
	order_sn string,
	first_order_time string,
	trading_price double,
	item_count int,
	channel_name string,
	latitude double,
	longitude double,
	create_time string,
	update_time string,
) partitioned by (first_order_time string,create_time string)


#用户首次消费门店表历史记录清洗
select 
tmpcusline.customer_id,
tc.mobilephone as customer_phone,
tmpcusline.store_id,
ts.`code` as storeno,
tor2.eshop_id as eshop_id,
ts.city_code as cityno,
a.`name` as city_name,
tmpcusline.order_sn,
tmpcusline.first_order_time,
tor2.trading_price as trading_price,
tor2.total_quantity as item_count,
tor2.channel_name as channel_name,
taddr.latitude,
taddr.longitude
from (
	select
	tor.customer_id ,
	tor.store_id ,
	min(tor.order_sn) as order_sn ,
	min(tor.create_time) as first_order_time
	from t_order tor
	join t_eshop te on (tor.eshop_id = te.id)
	where te.`name` NOT LIKE '%测试%' AND te.white!='QA'
	group by tor.customer_id ,tor.store_id 
) tmpcusline
join t_store ts on (tmpcusline.store_id = ts.id)
left join t_sys_area a on (ts.city_code = a.code and a.level = 2)
left join t_customer tc on (tmpcusline.customer_id = tc.id)
LEFT JOIN (	
	select tor.total_quantity,tor.trading_price,tor.store_id,tor.eshop_id,tor.order_sn,ts.city_code,tdc.name as channel_name,tor.order_address_id 
	from gemini.t_order tor 
	join gemini.t_eshop te on (tor.eshop_id = te.id)
	LEFT JOIN gemini.t_store ts ON (ts.id = tor.store_id)
	LEFT JOIN gemini.t_department_channel tdc ON (te.channel_id = tdc.id)
) tor2 on (tor2.order_sn = tmpcusline.order_sn)
LEFT JOIN t_order_address taddr ON (tor2.order_address_id = taddr.id)