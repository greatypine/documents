select 
T_0.store_city_name as '城市',
T_0.store_name as '门店',
T_0.department_name as '频道',
ifnull(T_1.thanTen,0) as '下单数超过10次的人数',
ifnull(T_2.lessTen,0) as '下单数 1-10 次的人数',
ifnull(T_3.tag1,0)  as  '商品类(人数)',
ifnull(T_3.tag2,0)  as  '服务类(人数)',
ifnull(T_3.tag3,0)  as  '团购类(人数)',
ifnull(T_3.tag4,0)  as  '非活动订单(人数)',
ifnull(T_4.tag5,0)  as  '商品类(金额)',
ifnull(T_4.tag6,0)  as  '服务类(金额)',
ifnull(T_4.tag7,0)  as  '团购类(金额)',
ifnull(T_4.tag8,0)  as  '非活动订单(金额)'
from 
(
	select 
	fnv_hash(concat(dmot.store_city_name ,dmot.store_name,dmot.department_name )) as id ,
	dmot.store_city_name ,
	dmot.store_name,
	dmot.department_name
	from
	daqweb.df_mass_order_total dmot
	where 1=1 
	and dmot.store_city_name is not null 
	and dmot.store_name is not null 
	and dmot.department_name is not null 
	and dmot.customer_id is not null 
	and dmot.sign_time > '2018-07-01'
	group by 
	dmot.store_city_name ,
	dmot.store_name,
	dmot.department_name
)T_0 
left join 
(
	select
	T_1_1.id,
	count(1) as thanTen
	from 
	(
		select 
		fnv_hash(concat(dmot.store_city_name ,dmot.store_name,dmot.department_name )) as id ,
		dmot.store_city_name ,
		dmot.store_name,
		dmot.department_name,
		dmot.customer_id,
		count(1) as num
		from
		daqweb.df_mass_order_total dmot
		where 1=1 
		and dmot.store_city_name is not null 
		and dmot.store_name is not null 
		and dmot.department_name is not null 
		and dmot.customer_id is not null 
		and dmot.sign_time > '2018-07-01'
		group by 
		dmot.store_city_name ,
		dmot.store_name,
		dmot.department_name,
		dmot.customer_id
		having count(1)>= 10
	)T_1_1 
	group by T_1_1.id
)T_1 on T_0.id = T_1.id
left join 
(
	select
	T_1_1.id,
	count(1) as lessTen
	from 
	(
		select 
		fnv_hash(concat(dmot.store_city_name ,dmot.store_name,dmot.department_name )) as id ,
		dmot.store_city_name ,
		dmot.store_name,
		dmot.department_name,
		dmot.customer_id,
		count(1) as num
		from
		daqweb.df_mass_order_total dmot
		where 1=1 
		and dmot.store_city_name is not null 
		and dmot.store_name is not null 
		and dmot.department_name is not null 
		and dmot.customer_id is not null 
		and dmot.sign_time > '2018-07-01'
		group by 
		dmot.store_city_name ,
		dmot.store_name,
		dmot.department_name,
		dmot.customer_id
		having count(1) >= 1 and count(1) < 10
	)T_1_1 
	group by T_1_1.id

)T_2 on T_0.id = T_2.id

left join 
(

	select 
	T_1_1.id ,
	max(case T_1_1.order_tag when '商品类' then T_1_1.tagnum else 0 end )     'tag1',
	max(case T_1_1.order_tag when '服务类' then T_1_1.tagnum else 0 end )     'tag2',
	max(case T_1_1.order_tag when '团购类' then T_1_1.tagnum else 0 end )     'tag3',
	max(case T_1_1.order_tag when '非活动订单' then T_1_1.tagnum else 0 end ) 'tag4'
	from 
	(
		select 
			fnv_hash(concat(dmot.store_city_name ,dmot.store_name,dmot.department_name )) as id ,
			 case
			 when dmot.order_tag2 = '1' then  '商品类'
			 when dmot.order_tag2 = '2' then  '服务类'
			 when dmot.order_tag2 = '3' then  '团购类'
			 else '非活动订单'
			 end as order_tag ,
			 count(distinct dmot.customer_id) as tagNum
		from
			daqweb.df_mass_order_total dmot
		where 1=1 
		and dmot.store_city_name is not null 
		and dmot.store_name is not null 
		and dmot.department_name is not null 
		and dmot.customer_id is not null 
		and dmot.sign_time > '2018-07-01'
		group by 
			dmot.store_city_name ,
			dmot.store_name,
			dmot.department_name,
			order_tag
	)T_1_1	
	group by T_1_1.id  

)T_3 on T_0.id = T_3.id
left join 
(

	select 
	T_1_1.id ,
	max(case T_1_1.order_tag when '商品类' then T_1_1.tagPrice else 0 end ) 'tag5',
	max(case T_1_1.order_tag when '服务类' then T_1_1.tagPrice else 0 end ) 'tag6',
	max(case T_1_1.order_tag when '团购类' then T_1_1.tagPrice else 0 end ) 'tag7',
	max(case T_1_1.order_tag when '非活动订单' then T_1_1.tagPrice else 0 end ) 'tag8'
	from 
	(
		select 
			fnv_hash(concat(dmot.store_city_name ,dmot.store_name,dmot.department_name )) as id ,
			 case
			 when dmot.order_tag2 = '1' then  '商品类'
			 when dmot.order_tag2 = '2' then  '服务类'
			 when dmot.order_tag2 = '3' then  '团购类'
			 else '非活动订单'
			 end as order_tag ,
			 sum(dmot.gmv_price) as tagPrice
		from
			daqweb.df_mass_order_total dmot
		where 1=1 
		and dmot.store_city_name is not null 
		and dmot.store_name is not null 
		and dmot.department_name is not null 
		and dmot.customer_id is not null 
		and dmot.sign_time > '2018-07-01'
		group by 
			dmot.store_city_name ,
			dmot.store_name,
			dmot.department_name,
			order_tag
	)T_1_1	
	group by T_1_1.id  


)T_4 on T_0.id = T_4.id
order by 
T_0.store_city_name ,
T_0.store_name,
T_0.department_name
