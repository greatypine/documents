INSERT  INTO  gabase.b_item_pro_total 


SELECT

toi.id ,
toi.order_id ,
toi.product_type ,
toi.eshop_pro_id ,
toi.eshop_pro_name ,
IFNULL(toi.unit_price,0.00) AS unit_price,
toi.unit_score ,
toi.quantity ,
toi.unit ,
toi.weight ,
toi.business_no ,
ifnull(toi.cost_price , 0.00) as cost_price ,
ifnull(toi.original_price , 0.00) as original_price ,
toi.provider_id ,
toi.output_tax_rate ,

tor.order_sn,
tor.order_status ,

tor.create_time,
'' as success_time,
tor.sign_time,
'' as cancel_time,
'' as commented_time,

tor.is_split,
tor.order_type,
tor.order_source,
tor.normal_store_id ,
tor.order_address_id,

dmot.info_village_code, 
dmot.area_code, 
dmot.info_employee_a_no, 


dmot.appointment_start_time,
dmot.appointment_end_time,
ifnull(tor.normal_store_id, tor.store_id) as real_store_id,
dmot.employee_no,
--------------------------new
dmot.order_tag3,
--------------------------new

customer_tb.name as customer_name,
customer_tb.mobilephone  as  customer_mobile_phone,

toa.name as addr_name,
toa.mobilephone as  addr_mobilephone,
toa.address as addr_address,

tor.employee_id,
tor.employee_phone,
tor.employee_name,

ts.code  as  store_code,
tsa.name as store_city_name,

tvc.tiny_village_id,
tvc.tiny_village_name,
taa.name as area_name,

dd.channel_id ,
dd.channel_name ,
dd.department_id,
dd.department_name ,

tbm.id AS business_model_id, 
tbm.name AS business_model_name,
tbm.code As business_code,

tep.id AS eshop_id,
tep.name AS eshop_name,
tep.self AS eshop_self,
tep.white AS eshop_white,
tep.create_time AS eshop_create_time,
tep.self_inventory ,
tep.send_type AS send_type,
tep.code AS eshop_code,
tep.eshop_kind ,
tep.area_company_id AS company_id,
tep.super_member,

--------------------------new
tep.joint_ims,
tep.business_type,
if(dcep.eshop_id is null  , 0 , 1 ) as teptag1,
--------------------------new

ts.id AS store_id,
ts.name AS store_name,
ts.province_code AS store_province_code,
ts.city_code AS store_city_code,
ts.ad_code AS store_ad_code,
ts.white AS store_white,
ts.create_time AS store_create_time,

ts.number as store_number,


IFNULL(tsr.name,'无')  AS sku_rule_name,
IFNULL(tsr.start_time,'2010-01-01') AS start_time,
IFNULL(tsr.end_time,'2010-01-01') AS end_time,

tsp.price AS sku_rule_price,

'' as rate,
0  as star_level,
'' as  contents,

0 as next_days,
''  AS next_contents,
0 AS star_level_1,
0 AS star_level_2,

customer_tb.customer_type,
customer_tb.customer_source,
customer_tb.create_time as customer_create_time,
customer_tb.customer_id,
customer_tb.plus_flag,
customer_tb.plus_expiry_date,
customer_tb.member_flag,
customer_tb.member_expiry_date,
customer_tb.member_type,
customer_tb.member_start_date,
customer_tb.associator_expiry_date,

'no' as usertag1,

toie.sum_trading_price,
toie.apportion_rebate,
toie.apportion_coupon,
toie.apportion_score,
toie.apportion_other,
toie.receive_rebate,  
(toie.apportion_coupon*tcoutype.proration_platform/100) as proration_platform_price,  
(toie.apportion_coupon*tcoutype.proration_seller/100) as   proration_seller_price   , 

tcoutype.proration_platform ,      
tcoutype.proration_seller ,    

tp.member_price,
tp.is_combo,
tp.is_combo_split,
vpc.level1_name ,
vpc.level2_name ,
vpc.level3_name ,
tp.content_bar,

tp.manager_category_ids     ,
tp.manager_category_names     ,
tp.ims_master_id     ,
imsT.level_1 as ims_level_1,
imsT.level_2 as ims_level_2,
imsT.level_3 as ims_level_3,
imsT.level_4 as ims_level_4,
imsT.level_5 as ims_level_5,
imsT.level_6 as ims_level_6,
imsT.level_7 as ims_level_7,


returned_tb.quantity  as   returned_quantity ,
returned_tb.price   as  returned_unit_price  ,
(returned_tb.quantity * returned_tb.price)  as   returned_price  ,


if(returned_tb.order_item_id is  null , 0 , 1 ) as return_label ,
tcou.content_type ,
tcou.content_no , 
tcoutype.content_name,
tcoutype.content_batch,


--------------------------new
contract.contract_end_date,
contract.contract_id,
contract.contract_method, 
contract.contract_percent, 
contract.contract_price,
if( tor.order_source= 'jdDaojia' , 
	toti.pro_price/100  ,  
   if(tor.is_split = 'yes' ,  
     IFNULL(toi.original_price,0.00) ,
 	 IFNULL(toi.unit_price,0.00)) )   AS trading_price,

ditdgd.c_guid,
ditdgd.c_gcode,
ditdgd.c_dt,
ditdgd.c_ccode,
ditdgd.c_type,
ditdgd.c_sale_status,
ifnull(ditdgd.cost_price , 0.00) as c_cost_price,
ditdgd.c_map_store_id,

--------------------------new

0 as rebuy_num,
'0' as item_type ,
cast(0.0 as double ) as order_profit ,
0 as pro_number ,
'' as extra_filed1    ,
'' as extra_filed2    ,
'' as extra_filed3    ,
'' as extra_filed4    ,
'' as extra_filed5    ,
'' as extra_filed6    ,
'' as extra_filed7    ,
'' as extra_filed8    ,
'' as extra_filed9    ,
'' as extra_filed10   ,
'' as extra_filed11   ,
'' as extra_filed12   ,
'' as extra_filed13   ,
'' as extra_filed14   ,
'' as extra_filed15  

FROM gemini.t_order_item toi 
JOIN gemini.t_order  tor ON tor.id=toi.order_id 
left join gemini.t_order_sq_tp tost on tor.id = tost.order_id 
left join gemini.t_order_tp_item toti on tost.tp_order_id = toti.tp_order_id and toi.eshop_pro_id = toti.product_id
left join gemini.t_order_address toa on tor.order_address_id = toa.id 
left join daqWeb.df_mass_order_total dmot on tor.id = dmot.id 
left join daqWeb.tiny_village_code tvc on dmot.info_village_code = tvc.code
left join daqWeb.t_area taa on dmot.area_code = taa.area_no
left join gemini.t_order_group tog on tor.group_id = tog.id
left join gemini.t_card_coupon tcou on tog.card_coupon_id = tcou.id
left join gemini.t_card_coupontype tcoutype on tcou.type_id = tcoutype.id
left join (
	select
	
		tc.customer_type,
		tc.customer_source,
		tc.create_time,
		tc.id AS customer_id,
		tc.plus_flag,
		tc.plus_expiry_date,
		tc.name ,
		if(dum.opencard_time is null , 'no' , 'yes') as  member_flag,
		dum.opencard_time  as member_start_date , 
		dum.associator_expiry_date as member_expiry_date,
		dum.member_type ,
		tc.associator_expiry_date,
		tc.mobilephone

	from gemini.t_customer tc 
	left join daqWeb.df_user_member dum on tc.id = dum.customer_id
	
)customer_tb on  customer_tb.customer_id=tor.customer_id
JOIN gemini.t_store ts ON ts.id=tor.store_id
left join gemini.t_sys_area tsa  on ts.city_code = tsa.code 
JOIN gemini.t_product tp ON tp.id=toi.eshop_pro_id 
left join 
(
	select 
	tpt.product_id,
	max(tpc1.name) as level1_name , 
	max(tpc2.name) as level2_name , 
	max(tpc3.name) as level3_name  
	from 
	gemini.t_product_tag tpt 
	left join gemini.t_product_category tpc1 on  tpt.category_id1 = tpc1.id 
	left join gemini.t_product_category tpc2 on  tpt.category_id2 = tpc2.id 
	left join gemini.t_product_category tpc3 on  tpt.category_id3 = tpc3.id 
	where tpt.category_id1 is not null 
	group by 
	tpt.product_id
)  vpc on tp.id = vpc.product_id
left join 
(
	select
		tip.id ,
		tip.code,
		tic1.name as level_1,
		tic2.name as level_2,
		tic3.name as level_3,
		tic4.name as level_4,
		tic5.name as level_5,
		tic6.name as level_6,
		tic7.name as level_7
	from gemini.t_ims_product tip
	join gemini.t_ims_product_category tipc on tip.id=tipc.pro_id
	join gemini.t_ims_category tic1 on tipc.category1=tic1.id
	join gemini.t_ims_category tic2 on tipc.category2=tic2.id
	join gemini.t_ims_category tic3 on tipc.category3=tic3.id
	join gemini.t_ims_category tic4 on tipc.category4=tic4.id
	join gemini.t_ims_category tic5 on tipc.category5=tic5.id
	join gemini.t_ims_category tic6 on tipc.category6=tic6.id
	join gemini.t_ims_category tic7 on tipc.category7=tic7.id
	where tip.circulation_state != '作废'
)imsT on tp.ims_master_id = imsT.id 
left join (
	 select 
		 c_guid,
		 c_gcode,
		 c_dt,
		 c_ccode,
		 c_type,
		 c_sale_status,
		 cost_price,
		 c_map_store_id
	 
	 from daqWeb.df_ims_tbs_d_gds_daily
) ditdgd on cast(ts.number  as string )= ditdgd.c_map_store_id and imsT.code = ditdgd.c_gcode and from_unixtime(unix_timestamp(ditdgd.c_dt), 'yyyy-MM-dd')  = from_unixtime(unix_timestamp(tor.sign_time), 'yyyy-MM-dd')

left join (
	select 
	order_item_id , 
	sum(quantity) as quantity ,
	max(price) as  price
	from gemini.t_order_returned 
	group by order_item_id
) returned_tb  on toi.id = returned_tb.order_item_id 
LEFT JOIN gemini.t_order_item_extra toie ON  toie.order_item_id=toi.id and toie.order_id=toi.order_id
LEFT JOIN gemini.t_eshop tep ON tep.id=tor.eshop_id
left join (
	SELECT 
		eshop_id,
		end_date as contract_end_date,
		id as contract_id,
		method as contract_method, 
		percent as contract_percent, 
		price as contract_price
	FROM (
			SELECT ROW_NUMBER() OVER(PARTITION BY tct.eshop_id  ORDER BY tct.end_date  DESC) rn,       
			tct.*
			FROM gemini.t_contract tct 
		  )t
	WHERE rn = 1    
)contract on tep.id = contract.eshop_id
LEFT JOIN (
	SELECT tep2.id AS department_id,
		   tep2.name AS department_name,
		   tep3.tep3_name AS channel_name,
		   tep3.tep3_id AS channel_id
	FROM gemini.t_department_channel tep2 JOIN
	  ( 
		SELECT
		tep3.parent_id,
        tep3.name AS tep3_name,
        tep3.id AS tep3_id
		FROM gemini.t_department_channel tep3
		WHERE tep3.level=2
	  ) tep3 ON tep2.id=tep3.parent_id
	WHERE tep2.level=1
) dd  ON tep.channel_id=dd.channel_id	 					
LEFT JOIN gemini.t_business_model tbm ON tbm.id=tep.business_model_id 
left join 
(
  select distinct eshop_id from daqweb.df_card_eshop_profit where isexempt  = 1
)dcep on tep.id = dcep.eshop_id 
LEFT JOIN gemini.t_sku_price_rule tsr ON tsr.id=toi.sku_rule_id 
LEFT JOIN gemini.t_sku_rule_product_ref tsp ON tsp.product_id=toi.eshop_pro_id AND toi.sku_rule_id=tsp.sku_rule_id AND tsp.status=0 

WHERE   tor.create_time>= to_date(date_sub(now(),1))   AND tor.create_time <= to_date(now()) 
  
AND  tep.white!='QA' AND ts.white!='QA' and ts.name not like '%测试%'
