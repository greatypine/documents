#用户首次消费门店建表语句
create table b_user_first_order_store(
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
	employee_no string,
	trading_price double,
	item_count int,
	channel_id string,
	channel_name string,
	department_id string,
	department_name string,
	sign_time string,
	latitude double,
	longitude double,
	create_time string,
	update_time string,
)


delete from b_user_first_order_store;
select count(*) from b_user_first_order_store;
--初始化历史数据
insert into gabase.b_user_first_order_store
select 
tmpcusline.customer_id,
tmpcusline.store_id,
tc.mobilephone as customer_phone,
ts.`code` as storeno,
tor2.eshop_id as eshop_id,
ts.city_code as cityno,
a.`name` as city_name,
tarea.area_no as area_code,
tor2.code as tiny_village_code,
tor2.employee_a_no as employee_a_no,
tor2.order_sn,
tmpcusline.first_order_time,
tor2.employee_no as employee_no,
tor2.trading_price as trading_price,
tor2.total_quantity as item_count,
tor2.channel_id as channel_id,
tor2.channel_name as channel_name,
tor2.department_id as department_id,
tor2.department_name as department_name,
tor2.sign_time,
taddr.latitude,
taddr.longitude,
cast(now() as string) as create_time,
cast(now() as string) as update_time
from (
    select customer_id,store_id,min(create_time) as first_order_time from (
    	select
    	tor.customer_id ,
    	ifnull(tor.normal_store_id,tor.store_id) as store_id,
    	tor.order_sn ,
    	tor.create_time
    	from gemini.t_order tor
    	join gemini.t_eshop te on (tor.eshop_id = te.id)
    	where te.`name` NOT LIKE '%测试%' AND te.white!='QA' and tor.sign_time is not null and tor.sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')
	) unitor group by customer_id,store_id
) tmpcusline
join gemini.t_store ts on (tmpcusline.store_id = ts.id)
left join gemini.t_sys_area a on (ts.city_code = a.code and a.level = 2)
left join gemini.t_customer tc on (tmpcusline.customer_id = tc.id)
LEFT JOIN (	
    select *,row_number() over (partition by customer_id,store_id order by create_time asc,sign_time asc) as rn
    from (
    	select tor.id,
    	tor.total_quantity,
    	tor.trading_price,
    	tor.customer_id,
    	ifnull(tor.normal_store_id,tor.store_id) as store_id,
    	tor.eshop_id,
    	tor.order_sn,
    	tor.create_time,
    	tor.sign_time,
    	ts.city_code,
    	tdc.id as channel_id,
    	tdc.name as channel_name,
    	tcc.id as department_id,
    	tcc.name as department_name,
    	tor.order_address_id,
    	ifnull(td.code,new.tiny_village_code) as code,
    	td.employee_a_no,
    	tem.code as employee_no
    	from gemini.t_order tor 
    	join gemini.t_eshop te on (tor.eshop_id = te.id)
    	LEFT JOIN gemini.t_store ts ON (ts.id = ifnull(tor.normal_store_id,tor.store_id))
    	LEFT JOIN gemini.t_department_channel tdc ON (te.channel_id = tdc.id)
    	LEFT JOIN gemini.t_department_channel tcc ON (te.bussiness_group_id = tcc.id)
    	LEFT JOIN gemini_mongo.tiny_dispatch td ON (td.orderid = tor.id)
    	LEFT JOIN gemini.t_employee tem ON (tem.id = tor.employee_id)
    	LEFT JOIN daqweb.df_customer_order_month_trade_new new ON (new.order_sn = tor.order_sn)
    	where tor.sign_time is not null and tor.sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')
	) t1 
) tor2 on (tor2.customer_id = tmpcusline.customer_id and tor2.store_id = tmpcusline.store_id and tor2.create_time = tmpcusline.first_order_time and tor2.rn = 1)
LEFT JOIN gemini.t_order_address taddr ON (tor2.order_address_id = taddr.id)
LEFT JOIN daqweb.tiny_area tarea ON (tarea.code = tor2.code and tarea.status = 0);

CREATE TABLE gabase.b_user_first_order_store_1 PRIMARY KEY (customer_id,store_id)  PARTITION BY HASH(customer_id) PARTITIONS  15 STORED AS KUDU  AS SELECT * FROM gabase.b_user_first_order_store;

ALTER TABLE gabase.b_user_first_order_store RENAME TO gabase.b_user_first_order_store_2;
ALTER TABLE gabase.b_user_first_order_store_1 RENAME TO gabase.b_user_first_order_store;

--更新小区为空的历史数据
upsert into b_user_first_order_store
(
    customer_id,
    store_id,
    area_code,
    tiny_village_code,
    employee_a_no,
    update_time
)
select 
    butm.customer_id,
    butm.store_id,
    butm.area_code,
    butm.tiny_village_code,
    butm.employee_a_no,
    butm.update_time
from b_user_first_order_store buf inner join 
(
select
    t1.customer_id,
    t1.store_id,
    tarea.area_no as area_code,
    tor2.code as tiny_village_code,
    tor2.employee_a_no  as employee_a_no,
    cast(now() as string) as update_time
    from 
(
    select customer_id,store_id from b_user_first_order_store where tiny_village_code is null or tiny_village_code = ''
) t1 inner join 
(
    select customer_id,store_id,min(create_time) as first_order_time from (
    	select
    	tor.customer_id ,
    	ifnull(tor.normal_store_id,tor.store_id) as store_id,
    	tor.order_sn ,
    	tor.create_time
    	from gemini.t_order tor
    	join gemini.t_eshop te on (tor.eshop_id = te.id)
    	join gemini_mongo.tiny_dispatch td ON (td.orderid = tor.id)
    	where td.code is not null and td.code != '' and te.`name` NOT LIKE '%测试%' AND te.white!='QA' and tor.sign_time is not null and tor.sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')
    	union
    	select
    	tor.customer_id ,
    	ifnull(tor.normal_store_id,tor.store_id) as store_id,
    	tor.order_sn,
    	tor.create_time
    	from gemini.t_order tor
    	join gemini.t_eshop te on (tor.eshop_id = te.id)
    	join daqweb.df_customer_order_month_trade_new td ON (td.order_sn = tor.order_sn)
    	where td.tiny_village_code is not null and td.tiny_village_code != '' and te.`name` NOT LIKE '%测试%' AND te.white!='QA' and tor.sign_time is not null and tor.sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')
	) unitor group by customer_id,store_id
) tmpcusline on (t1.customer_id = tmpcusline.customer_id and t1.store_id = tmpcusline.store_id)
LEFT JOIN (	
	select *,row_number() over (partition by customer_id,store_id order by create_time asc,sign_time asc) as rn
    from (
    	select tor.id,
    	tor.total_quantity,
    	tor.trading_price,
    	tor.customer_id,
    	ifnull(tor.normal_store_id,tor.store_id) as store_id,
    	tor.eshop_id,
    	tor.order_sn,
    	tor.create_time,
    	tor.sign_time,
    	ts.city_code,
    	tdc.id as channel_id,
    	tdc.name as channel_name,
    	tcc.id as department_id,
    	tcc.name as department_name,
    	tor.order_address_id,
    	ifnull(td.code,new.tiny_village_code) as code,
    	td.employee_a_no,
    	tem.code as employee_no
    	from gemini.t_order tor 
    	join gemini.t_eshop te on (tor.eshop_id = te.id)
    	LEFT JOIN gemini.t_store ts ON (ts.id = ifnull(tor.normal_store_id,tor.store_id))
    	LEFT JOIN gemini.t_department_channel tdc ON (te.channel_id = tdc.id)
    	LEFT JOIN gemini.t_department_channel tcc ON (te.bussiness_group_id = tcc.id)
    	LEFT JOIN gemini_mongo.tiny_dispatch td ON (td.orderid = tor.id)
    	LEFT JOIN gemini.t_employee tem ON (tem.id = tor.employee_id)
    	LEFT JOIN daqweb.df_customer_order_month_trade_new new ON (new.order_sn = tor.order_sn)
    	where tor.sign_time is not null and tor.sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')
	) t1 
) tor2 on (tor2.customer_id = tmpcusline.customer_id and tor2.store_id = tmpcusline.store_id and tor2.create_time = tmpcusline.first_order_time and tor2.rn = 1)
LEFT JOIN daqweb.tiny_area tarea ON (tarea.code = tor2.code and tarea.status = 0)
) butm on (buf.customer_id = butm.customer_id and buf.store_id = butm.store_id);

--更新channel_id为空的历史数据

upsert into b_user_first_order_store
(
    customer_id,
    store_id,
    channel_id,
    channel_name,
    department_id,
    department_name,
    update_time
)
select buf.customer_id,
    buf.store_id,
    t.channel_id,
    t.channel_name,
    t.department_id,
    t.department_name,
    cast(now() as string) as update_time
    from b_user_first_order_store buf inner join 
(
    select tmp.*,
        row_number() over (partition by tmp.customer_id,tmp.store_id order by tmp.create_time asc, tmp.sign_time asc) as rn
     from (
    	select 
    		tor.customer_id,
    		ifnull(tor.normal_store_id,tor.store_id) as store_id,
    		tor.sign_time,
    		tor.create_time,
    		tdc.id as channel_id,
    		tdc.name as channel_name,
    		tcc.id as department_id,
    		tcc.name as department_name
    	from gemini.t_order tor 
    	left join gemini.t_eshop te on (tor.eshop_id = te.id)
    	LEFT JOIN gemini.t_store ts ON (ts.id = tor.store_id)
    	LEFT JOIN gemini.t_department_channel tdc ON (te.channel_id = tdc.id)
    	LEFT JOIN gemini.t_department_channel tcc ON (te.bussiness_group_id = tcc.id)
    	where tor.sign_time is not null and tor.sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') 
    	and te.channel_id is not null and te.bussiness_group_id is not null 
    ) tmp
) t on (buf.customer_id = t.customer_id and buf.store_id = t.store_id)
where buf.channel_id is null and t.rn = 1 



--每日插入数据hql
insert into b_user_first_order_store
select 
tmpcusline.customer_id,
tmpcusline.store_id,
tc.mobilephone as customer_phone,
ts.`code` as storeno,
tor2.eshop_id as eshop_id,
ts.city_code as cityno,
a.`name` as city_name,
tarea.area_no as area_code,
tor2.code as tiny_village_code,
tor2.employee_a_no as employee_a_no,
tor2.order_sn,
tmpcusline.first_order_time,
tor2.employee_no as employee_no,
tor2.trading_price as trading_price,
tor2.total_quantity as item_count,
tor2.channel_id as channel_id,
tor2.channel_name as channel_name,
tor2.department_id as department_id,
tor2.department_name as department_name,
tor2.sign_time as sign_time,
taddr.latitude,
taddr.longitude,
cast(now() as string) as create_time,
cast(now() as string) as update_time
from (
    select 	
        customer_id ,
    	store_id,
    	min(create_time) as first_order_time 
    from (
    	select
    	tor.customer_id ,
    	ifnull(tor.normal_store_id,store_id) as store_id,
    	tor.order_sn ,
    	tor.create_time
    	from gemini.t_order tor
    	join gemini.t_eshop te on (tor.eshop_id = te.id)
    	where te.`name` NOT LIKE '%测试%' AND te.white!='QA' and tor.sign_time is not null 
    	and tor.sign_time >= concat(from_unixtime(unix_timestamp(date_sub(now(), 1)),"yyyy-MM-dd"), ' 00:00:00')
        AND tor.sign_time < concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')
    ) m group by customer_id,store_id 
) tmpcusline
join gemini.t_store ts on (tmpcusline.store_id = ts.id)
left join gemini.t_sys_area a on (ts.city_code = a.code and a.level = 2)
left join gemini.t_customer tc on (tmpcusline.customer_id = tc.id)
LEFT JOIN (	
    select *,row_number() over (partition by customer_id,store_id order by create_time asc,sign_time asc) as rn
    from (
    	select tor.id,
    	tor.total_quantity,
    	tor.trading_price,
    	tor.customer_id,
    	ifnull(tor.normal_store_id,tor.store_id) as store_id,
    	tor.eshop_id,
    	tor.order_sn,
    	tor.create_time,
    	tor.sign_time,
    	ts.city_code,
    	tdc.id as channel_id,
    	tdc.name as channel_name,
    	tcc.id as department_id,
    	tcc.name as department_name,
    	tor.order_address_id,
    	ifnull(td.code,new.tiny_village_code) as code,
    	td.employee_a_no,
    	tem.code as employee_no
    	from gemini.t_order tor 
    	join gemini.t_eshop te on (tor.eshop_id = te.id)
    	LEFT JOIN gemini.t_store ts ON (ts.id = ifnull(tor.normal_store_id,tor.store_id))
    	LEFT JOIN gemini.t_department_channel tdc ON (te.channel_id = tdc.id)
    	LEFT JOIN gemini.t_department_channel tcc ON (te.bussiness_group_id = tcc.id)
    	LEFT JOIN gemini_mongo.tiny_dispatch td ON (td.orderid = tor.id)
    	LEFT JOIN gemini.t_employee tem ON (tem.id = tor.employee_id)
    	LEFT JOIN daqweb.df_customer_order_month_trade_new new ON (new.order_sn = tor.order_sn)
    	where tor.sign_time is not null 
        and tor.sign_time >= concat(from_unixtime(unix_timestamp(date_sub(now(), 1)),"yyyy-MM-dd"), ' 00:00:00')
        AND tor.sign_time < concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')
	) t1 
) tor2 on (tor2.customer_id = tmpcusline.customer_id and tor2.store_id = tmpcusline.store_id and tor2.create_time = tmpcusline.first_order_time and tor2.rn = 1)
LEFT JOIN gemini.t_order_address taddr ON (tor2.order_address_id = taddr.id)
LEFT JOIN daqweb.tiny_area tarea ON (tarea.code = tor2.code and tarea.status = 0)
where concat(tmpcusline.customer_id,tmpcusline.store_id) not in (select concat(customer_id,store_id) from b_user_first_order_store)
;

--更新小区坐标为空的用户
upsert into b_user_first_order_store 
(
    customer_id,
    store_id,
    area_code,
    tiny_village_code,
    employee_a_no,
    update_time
)
select buf.customer_id,buf.store_id,tiny.area_code,tiny.tiny_village_code,tiny.employee_a_no,tiny.update_time from b_user_first_order_store buf 
inner join (
    select 
    tor2.customer_id,
    tor2.store_id,
    tarea.area_no as area_code,
    tor2.code as tiny_village_code,
    tor2.employee_a_no as employee_a_no,
    cast(now() as string) as update_time
    from (	
    select *,row_number() over (partition by customer_id,store_id order by create_time asc,sign_time asc) as rn
        from (
        	select
        	tor.customer_id,
        	ifnull(tor.normal_store_id,tor.store_id) as store_id,
        	tor.create_time,
        	tor.sign_time,
        	tor.order_address_id,
        	td.code,
        	td.employee_a_no
        	from gemini.t_order tor 
        	join gemini.t_eshop te on (tor.eshop_id = te.id)
        	LEFT JOIN gemini.t_store ts ON (ts.id = ifnull(tor.normal_store_id,tor.store_id))
        	LEFT JOIN gemini_mongo.tiny_dispatch td ON (td.orderid = tor.id)
        	where tor.sign_time is not null 
            and tor.sign_time >= concat(from_unixtime(unix_timestamp(date_sub(now(), 1)),"yyyy-MM-dd"), ' 00:00:00')
            AND tor.sign_time < concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')
    	) t1 
    ) tor2
    LEFT JOIN daqweb.tiny_area tarea ON (tarea.code = tor2.code and tarea.status = 0)
    where tor2.rn =1 and tor2.code is not null and concat(tor2.customer_id,tor2.store_id) in (select concat(customer_id,store_id) from b_user_first_order_store where tiny_village_code is null or tiny_village_code = '')
) tiny on (buf.customer_id = tiny.customer_id and buf.store_id = tiny.store_id)

--更新频道为空的用户
upsert into b_user_first_order_store 
(
    customer_id,
    store_id,
    channel_id,
    channel_name,
    department_id,
    department_name,
    update_time
)
select buf.customer_id,
    buf.store_id,
    channel.channel_id,
    channel.channel_name,
    channel.department_id,
    channel.department_name,
    channel.update_time
from b_user_first_order_store buf 
inner join (
select 
tor2.customer_id,
tor2.store_id,
tor2.channel_id,
tor2.channel_name,
tor2.department_id,
tor2.department_name,
cast(now() as string) as update_time
from (	
select *,row_number() over (partition by customer_id,store_id order by create_time asc,sign_time asc) as rn
    from (
    	select
    	tor.customer_id,
    	ifnull(tor.normal_store_id,tor.store_id) as store_id,
    	tor.create_time,
    	tor.sign_time,
    	tdc.id as channel_id,
    	tdc.name as channel_name,
    	tcc.id as department_id,
    	tcc.name as department_name
    	from gemini.t_order tor 
    	join gemini.t_eshop te on (tor.eshop_id = te.id)
    	LEFT JOIN gemini.t_store ts ON (ts.id = ifnull(tor.normal_store_id,tor.store_id))
    	LEFT JOIN gemini.t_department_channel tdc ON (te.channel_id = tdc.id)
    	LEFT JOIN gemini.t_department_channel tcc ON (te.bussiness_group_id = tcc.id)
    	LEFT JOIN gemini_mongo.tiny_dispatch td ON (td.orderid = tor.id)
    	LEFT JOIN gemini.t_employee tem ON (tem.id = tor.employee_id)
    	LEFT JOIN daqweb.df_customer_order_month_trade_new new ON (new.order_sn = tor.order_sn)
    	where tor.sign_time is not null 
        and tor.sign_time >= concat(from_unixtime(unix_timestamp(date_sub(now(), 1)),"yyyy-MM-dd"), ' 00:00:00')
        AND tor.sign_time < concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')
	) t1 
) tor2 
where tor2.rn =1 and tor2.channel_id is not null and concat(tor2.customer_id,tor2.store_id) in (select concat(customer_id,store_id) from b_user_first_order_store where channel_id is null)
) channel on (buf.customer_id = channel.customer_id and buf.store_id = channel.store_id);





drop view if exists v_full_success_order;
create view v_full_success_order as 
SELECT
	o.id order_id,
	o.order_sn,
	o.group_id order_group_id,
	o.order_type,
	o.business_model_id,
	o.customer_id,
	o.order_address_id,
	ifnull(o.normal_store_id, o.store_id) as store_id,
	o.store_id as order_store_id,
	o.eshop_id,
	o.order_status,
	o.order_source,
	o.invoice_status,
	o.buyer_remark,
	o.seller_remark,
	o.employee_remark,
	o.store_remark,
	o.abnormal_type,
	ifnull(o.abnormal_remark, '') abnormal_remark,
	o.delivery_type,
	o.trading_price,
	o.payable_price,
	o.score order_score,
	o.is_split,
	o.employee_id,
	o.employee_phone,
	o.employee_name,
	o.appointment_start_time,
	o.appointment_end_time,
	o.eshop_combo_pro_id,
	o.expiry_date,
	o.combo_price,
	o.total_quantity,
	o.groupon_instance_id,
	o. STATUS order_del_status,
	o.version order_version,
	o.create_user order_create_user,
	o.create_time order_create_time,
	o.update_user order_update_user,
	o.update_time order_update_time,
	o.create_user_id order_create_user_id,
	o.update_user_id order_update_user_id,
	o.order_sn_reserve,
	o.third_part order_third_part,
	o.sign_time,
	s. NAME store_name,
	s.address store_address,
	s.manager_id,
	s.mobilephone store_mobilephone,
	s.telephone store_telephone,
	s.service_phone,
	concat(s.province_code, '000000') store_province_code,
	concat(
		strleft (s.ad_code, 4),
		'00000000'
	) store_city_code,
	concat(s.ad_code, '000000') store_county_code,
	s.city_code store_city_area_code,
	s.logo_url,
	s.number,
	s.white store_white,
	s.cloud_map_flag,
	s. STATUS store_del_status,
	s.version store_version,
	s.create_user_id store_create_user_id,
	s.create_user store_create_user,
	s.create_time store_create_time,
	s.update_user_id store_update_user_id,
	s.update_user store_update_user,
	s.update_time store_update_time,
	s. CODE store_code,
	e. NAME eshop_name,
	e.seller_id,
	e.telephone eshop_telephone,
	e.content_img,
	e.`sort`,
	e.detail_address eshop_detail_address,
	e.order_lagtime,
	e.self,
	e.business_type,
	e.publish,
	e. STATUS eshop_del_status,
	e.freeze_status,
	e.business_status,
	e.white eshop_white,
	e.version eshop_version,
	e.create_user eshop_create_user,
	e.create_time eshop_create_time,
	e.update_user eshop_update_user,
	e.update_time eshop_update_time,
	e.create_user_id eshop_create_user_id,
	e.update_user_id eshop_update_user_id,
	e.fail_reason,
	e.type eshop_type,
	e.department_id,
	e.self_inventory,
	e.month_sale_self,
	e.invoice,
	e.support_bar,
	e.freight,
	e.free_freight_limit,
	e.dispatch_limit,
	e.send_type,
	e.month_sale,
	e. CODE eshop_code,
	e.product_review,
	e.order_userinfo_visible,
	e.compensation_first,
	e.eshop_kind,
	e.need_entering_idcard,
	e.need_delivery_time,
	e.export_report,
	concat(e.province_code, '000000') eshop_province_code,
	concat(
		strleft (e.area_code, 4),
		'00000000'
	) eshop_city_code,
	concat(e.area_code, '000000') eshop_county_code,
	e.city_code eshop_city_area_code,
	e.contrast_data,
	e.operation_base_status,
	e.certificate_status,
	e.is_pass,
	e.notice_type,
	e.third_part eshop_third_part,
	e.operation_base_reason,
	e.only_special_balance,
	e.operation_first_userid,
	e.operation_first_time,
	e.auto_confirm_transfer,
	e.taxpayer_num_id,
	ifnull(e.bussiness_group_id, '') bussiness_group_id,
	ifnull(e.channel_id, '') channel_id,
	e.area_company_id,
	e.is_plus,
	e.joint_ims,
	ifnull(c.short_name, '') cust_short_name,
	c.associator_level,
	c.associator_expiry_date,
	c. PASSWORD cust_password,
	ifnull(c. NAME, '') cust_name,
	c.sex,
	c.birthday,
	ifnull(c.mobilephone, '') cust_mobilephone,
	c.customer_type,
	c.customer_source,
	c.citic_open_id,
	c.telephone cust_telephone,
	c.mail,
	c.black_flg,
	c.avatar,
	c.last_login,
	c.device_os,
	c.app_token,
	c.white cust_white,
	c.longitude cust_longitude,
	c.latitude cust_latitude,
	c.storeId,
	c.wx_pub_open_id,
	c.score cust_score,
	c.growth,
	c.grade,
	c.plus_flag,
	c.plus_expiry_date,
	c.member_flag,
	c.member_expiry_date,
	c.remark,
	c. STATUS cust_del_status,
	c.version cust_version,
	c.create_user_id cust_create_user_id,
	c.create_user cust_create_user,
	c.create_time cust_create_time,
	c.update_user_id cust_update_user_id,
	c.update_user cust_update_user,
	c.update_time cust_update_time,
	c.account,
	c.city_code cust_city_area_code,
	dc.number dc_number,
	dc. NAME dc_name,
	dc. CODE dc_code,
	dc.parent_id dc_parent_id,
	dc. LEVEL dc_level,
	dc. STATUS dc_del_status,
	dc.version dc_version,
	dc.create_user dc_create_user,
	dc.create_time dc_create_time,
	dc.update_user dc_update_user,
	dc.update_time dc_update_time,
	dc.create_user_id dc_create_user_id,
	dc.update_user_id dc_update_user_id,
	dcc.number dcc_number,
	dcc. NAME dcc_name,
	dcc. CODE dcc_code,
	dcc.parent_id dcc_parent_id,
	dcc. LEVEL dcc_level,
	dcc. STATUS dcc_del_status,
	dcc.version dcc_version,
	dcc.create_user dcc_create_user,
	dcc.create_time dcc_create_time,
	dcc.update_user dcc_update_user,
	dcc.update_time dcc_update_time,
	dcc.create_user_id dcc_create_user_id,
	dcc.update_user_id dcc_update_user_id,
	cp.number cp_number,
	cp. NAME cp_name,
	cp.short_name cp_short_name,
	cp.type cp_type,
	cp.address cp_address,
	cp. CODE cp_code,
	concat(cp.province_code, '000000') cp_province_code,
	cp.city_code cp_city_area_code,
	cp.telephone cp_telephone,
	cp.electronic_invoice_flag,
	cp.drawer,
	cp.taxpayer_num,
	cp.bank_name,
	cp.bank_account,
	cp. STATUS cp_del_status,
	cp.version cp_version,
	cp.create_user cp_create_user,
	cp.create_time cp_create_time,
	cp.update_user cp_update_user,
	cp.update_time cp_update_time,
	cp.create_user_id cp_create_user_id,
	cp.update_user_id cp_update_user_id,
	concat(oa.province_code, '000000') oa_province_code,
	oa.province_name oa_province_name,
	concat(
		strleft (oa.ad_code, 4),
		'000000'
	) oa_city_code,
	oa.city_name oa_city_name,
	concat(oa.ad_code, '000000	') oa_county_code,
	oa.ad_name oa_county_name,
	oa.city_code oa_city_area_code,
	oa.placename,
	oa.latitude oa_latitude,
	oa.longitude oa_longitude,
	oa.detail_address oa_detail_address,
	oa.address oa_address,
	oa.zipcode,
	oa. NAME oa_name,
	oa.mobilephone oa_mobilephone,
	oa. STATUS oa_del_status,
	oa.version oa_version,
	oa.create_user_id oa_create_user_id,
	oa.create_user oa_create_user,
	oa.create_time oa_create_time,
	oa.update_user_id oa_update_user_id,
	oa.update_user oa_update_user,
	oa.update_time oa_update_time,
	oa.express_code,
	oa.express_name,
	oa.tracking_number,
	oa.delivery_time,
	td.code,
	area.area_no
FROM
	gemini.t_order o
LEFT OUTER JOIN gemini.t_store s ON s.id = ifnull(o.normal_store_id, o.store_id)
LEFT OUTER JOIN gemini.t_eshop e ON e.id = o.eshop_id
LEFT OUTER JOIN gemini.t_customer c ON c.id = o.customer_id
LEFT OUTER JOIN gemini.t_department_channel dc ON dc.id = e.bussiness_group_id
LEFT OUTER JOIN gemini.t_department_channel dcc ON dcc.id = e.channel_id
LEFT OUTER JOIN gemini.t_company cp ON cp.id = e.area_company_id
LEFT OUTER JOIN gemini.t_order_address oa ON oa.id = o.order_address_id
LEFT OUTER JOIN gemini_mongo.tiny_dispatch td ON (td.orderid = o.id)
LEFT OUTER JOIN daqweb.tiny_area area ON (area.code = td.code and area.status = 0)
WHERE
	(
		NOT o.abnormal_remark LIKE '%测试%'
		OR o.abnormal_remark IS NULL
	)
AND o.order_status IN (
	'signed',
	'commented',
	'success'
)
AND e.white != 'QA'
AND NOT e. NAME LIKE '%测试%'
AND (
	(s.white != 'QA' AND s. STATUS = 0)
	OR s.id = '00000000000000000000000000000084'
	OR s.id = '8ad89587594002650159691cb0564153'
	OR s.id = '8ad8c18a58bd5dbb015912cfb5c54605'
)
AND NOT s. NAME LIKE '%测试%'
AND c.white != 'QA'
AND (
	NOT c.short_name LIKE '%测试%'
	OR c.short_name IS NULL
)




create view v_full_order as 
SELECT
	o.id order_id,
	o.order_sn,
	o.group_id order_group_id,
	o.order_type,
	o.business_model_id,
	o.customer_id,
	o.order_address_id,
	ifnull(o.normal_store_id, o.store_id) as store_id,
	o.store_id as order_store_id,
	o.eshop_id,
	o.order_status,
	o.order_source,
	o.invoice_status,
	o.buyer_remark,
	o.seller_remark,
	o.employee_remark,
	o.store_remark,
	o.abnormal_type,
	ifnull(o.abnormal_remark, '') abnormal_remark,
	o.delivery_type,
	o.trading_price,
	o.payable_price,
	o.score order_score,
	o.is_split,
	o.employee_id,
	o.employee_phone,
	o.employee_name,
	o.appointment_start_time,
	o.appointment_end_time,
	o.eshop_combo_pro_id,
	o.expiry_date,
	o.combo_price,
	o.total_quantity,
	o.groupon_instance_id,
	o. STATUS order_del_status,
	o.version order_version,
	o.create_user order_create_user,
	o.create_time order_create_time,
	o.update_user order_update_user,
	o.update_time order_update_time,
	o.create_user_id order_create_user_id,
	o.update_user_id order_update_user_id,
	o.order_sn_reserve,
	o.third_part order_third_part,
	o.sign_time,
	s. NAME store_name,
	s.address store_address,
	s.manager_id,
	s.mobilephone store_mobilephone,
	s.telephone store_telephone,
	s.service_phone,
	concat(s.province_code, '000000') store_province_code,
	concat(
		strleft (s.ad_code, 4),
		'00000000'
	) store_city_code,
	concat(s.ad_code, '000000') store_county_code,
	s.city_code store_city_area_code,
	s.logo_url,
	s.number,
	s.white store_white,
	s.cloud_map_flag,
	s. STATUS store_del_status,
	s.version store_version,
	s.create_user_id store_create_user_id,
	s.create_user store_create_user,
	s.create_time store_create_time,
	s.update_user_id store_update_user_id,
	s.update_user store_update_user,
	s.update_time store_update_time,
	s. CODE store_code,
	e. NAME eshop_name,
	e.seller_id,
	e.telephone eshop_telephone,
	e.content_img,
	e.`sort`,
	e.detail_address eshop_detail_address,
	e.order_lagtime,
	e.self,
	e.business_type,
	e.publish,
	e. STATUS eshop_del_status,
	e.freeze_status,
	e.business_status,
	e.white eshop_white,
	e.version eshop_version,
	e.create_user eshop_create_user,
	e.create_time eshop_create_time,
	e.update_user eshop_update_user,
	e.update_time eshop_update_time,
	e.create_user_id eshop_create_user_id,
	e.update_user_id eshop_update_user_id,
	e.fail_reason,
	e.type eshop_type,
	e.department_id,
	e.self_inventory,
	e.month_sale_self,
	e.invoice,
	e.support_bar,
	e.freight,
	e.free_freight_limit,
	e.dispatch_limit,
	e.send_type,
	e.month_sale,
	e. CODE eshop_code,
	e.product_review,
	e.order_userinfo_visible,
	e.compensation_first,
	e.eshop_kind,
	e.need_entering_idcard,
	e.need_delivery_time,
	e.export_report,
	concat(e.province_code, '000000') eshop_province_code,
	concat(
		strleft (e.area_code, 4),
		'00000000'
	) eshop_city_code,
	concat(e.area_code, '000000') eshop_county_code,
	e.city_code eshop_city_area_code,
	e.contrast_data,
	e.operation_base_status,
	e.certificate_status,
	e.is_pass,
	e.notice_type,
	e.third_part eshop_third_part,
	e.operation_base_reason,
	e.only_special_balance,
	e.operation_first_userid,
	e.operation_first_time,
	e.auto_confirm_transfer,
	e.taxpayer_num_id,
	ifnull(e.bussiness_group_id, '') bussiness_group_id,
	ifnull(e.channel_id, '') channel_id,
	e.area_company_id,
	e.is_plus,
	e.joint_ims,
	ifnull(c.short_name, '') cust_short_name,
	c.associator_level,
	c.associator_expiry_date,
	c. PASSWORD cust_password,
	ifnull(c. NAME, '') cust_name,
	c.sex,
	c.birthday,
	ifnull(c.mobilephone, '') cust_mobilephone,
	c.customer_type,
	c.customer_source,
	c.citic_open_id,
	c.telephone cust_telephone,
	c.mail,
	c.black_flg,
	c.avatar,
	c.last_login,
	c.device_os,
	c.app_token,
	c.white cust_white,
	c.longitude cust_longitude,
	c.latitude cust_latitude,
	c.storeId,
	c.wx_pub_open_id,
	c.score cust_score,
	c.growth,
	c.grade,
	c.plus_flag,
	c.plus_expiry_date,
	c.member_flag,
	c.member_expiry_date,
	c.remark,
	c. STATUS cust_del_status,
	c.version cust_version,
	c.create_user_id cust_create_user_id,
	c.create_user cust_create_user,
	c.create_time cust_create_time,
	c.update_user_id cust_update_user_id,
	c.update_user cust_update_user,
	c.update_time cust_update_time,
	c.account,
	c.city_code cust_city_area_code,
	dc.number dc_number,
	dc. NAME dc_name,
	dc. CODE dc_code,
	dc.parent_id dc_parent_id,
	dc. LEVEL dc_level,
	dc. STATUS dc_del_status,
	dc.version dc_version,
	dc.create_user dc_create_user,
	dc.create_time dc_create_time,
	dc.update_user dc_update_user,
	dc.update_time dc_update_time,
	dc.create_user_id dc_create_user_id,
	dc.update_user_id dc_update_user_id,
	dcc.number dcc_number,
	dcc. NAME dcc_name,
	dcc. CODE dcc_code,
	dcc.parent_id dcc_parent_id,
	dcc. LEVEL dcc_level,
	dcc. STATUS dcc_del_status,
	dcc.version dcc_version,
	dcc.create_user dcc_create_user,
	dcc.create_time dcc_create_time,
	dcc.update_user dcc_update_user,
	dcc.update_time dcc_update_time,
	dcc.create_user_id dcc_create_user_id,
	dcc.update_user_id dcc_update_user_id,
	cp.number cp_number,
	cp. NAME cp_name,
	cp.short_name cp_short_name,
	cp.type cp_type,
	cp.address cp_address,
	cp. CODE cp_code,
	concat(cp.province_code, '000000') cp_province_code,
	cp.city_code cp_city_area_code,
	cp.telephone cp_telephone,
	cp.electronic_invoice_flag,
	cp.drawer,
	cp.taxpayer_num,
	cp.bank_name,
	cp.bank_account,
	cp. STATUS cp_del_status,
	cp.version cp_version,
	cp.create_user cp_create_user,
	cp.create_time cp_create_time,
	cp.update_user cp_update_user,
	cp.update_time cp_update_time,
	cp.create_user_id cp_create_user_id,
	cp.update_user_id cp_update_user_id,
	concat(oa.province_code, '000000') oa_province_code,
	oa.province_name oa_province_name,
	concat(
		strleft (oa.ad_code, 4),
		'000000'
	) oa_city_code,
	oa.city_name oa_city_name,
	concat(oa.ad_code, '000000	') oa_county_code,
	oa.ad_name oa_county_name,
	oa.city_code oa_city_area_code,
	oa.placename,
	oa.latitude oa_latitude,
	oa.longitude oa_longitude,
	oa.detail_address oa_detail_address,
	oa.address oa_address,
	oa.zipcode,
	oa. NAME oa_name,
	oa.mobilephone oa_mobilephone,
	oa. STATUS oa_del_status,
	oa.version oa_version,
	oa.create_user_id oa_create_user_id,
	oa.create_user oa_create_user,
	oa.create_time oa_create_time,
	oa.update_user_id oa_update_user_id,
	oa.update_user oa_update_user,
	oa.update_time oa_update_time,
	oa.express_code,
	oa.express_name,
	oa.tracking_number,
	oa.delivery_time,
	td.code,
	area.area_no
FROM
	gemini.t_order o
LEFT OUTER JOIN gemini.t_store s ON s.id = ifnull(o.normal_store_id, o.store_id)
LEFT OUTER JOIN gemini.t_eshop e ON e.id = o.eshop_id
LEFT OUTER JOIN gemini.t_customer c ON c.id = o.customer_id
LEFT OUTER JOIN gemini.t_department_channel dc ON dc.id = e.bussiness_group_id
LEFT OUTER JOIN gemini.t_department_channel dcc ON dcc.id = e.channel_id
LEFT OUTER JOIN gemini.t_company cp ON cp.id = e.area_company_id
LEFT OUTER JOIN gemini.t_order_address oa ON oa.id = o.order_address_id
LEFT OUTER JOIN gemini_mongo.tiny_dispatch td ON (td.orderid = o.id)
LEFT OUTER JOIN daqweb.tiny_area area ON (area.code = td.code and area.status = 0)
WHERE
 e.white != 'QA'
AND NOT e. NAME LIKE '%测试%'




create view v_pay_order as 
select tor.*,torp.is_clearing,torp.payment,torp.pay_platform,torp.pay_status,torp.app_pay_succeed,torp.pay_time,torp.pay_source
from gemini.t_order tor inner join (
    select *,row_number() over (partition by order_group_id order by create_time desc) as rn from gemini.t_order_receipts
) torp on (tor.group_id = torp.order_group_id and rn = 1) 