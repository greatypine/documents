select 
tc.id as customer_id,
tc.mobilephone as mobilephone,
tca.idcard as idcard,
case when tca.idcard is not null and tca.idcard != '' then strleft(tca.idcard,6) else null end 
as birthplace,
case when tca.idcard is not null and tca.idcard != '' then strleft(tca.idcard,2) else null end 
as born_province,
case when tca.idcard is not null and tca.idcard != '' then strleft(tca.idcard,4) else null end 
as born_city,
tca.birthday as birthday,
tc.create_time as regist_time,
tc.associator_level as associator_level,
tc.white as white,
case when torp.pay_time is not null and torp.pay_time != '' then torp.pay_time when tca.create_time is not null and tca.create_time != '' then tca.create_time else '' end
as opencard_time,
case when torp.pay_time is not null and torp.pay_time != '' then torp.pay_time when tca.create_time is not null and tca.create_time != '' then tca.create_time else '' end
as first_opencard_time,
case when torp.store_id is not null and torp.store_id != '' then torp.store_id when tcard.store_id is not null and tcard.store_id != '' then tcard.store_id else '' end 
as regist_storeid,
case when torp.city_code is not null and torp.city_code != '' then ts.city_code when tcard.city_code  is not null and tcard.city_code != '' then tcard.city_code else tca.city_code end 
as regist_cityno,
case when torp.type is not null and torp.type != '' then torp.type else tcard.type end as member_type, 
tc.associator_expiry_date as associator_expiry_date,
tc.customer_source as customer_source
from gemini.t_customer tc 
LEFT JOIN gabase.b_customer_associator tca ON (tc.customer_id = tca.customer_id)
LEFT JOIN (
	select t.customer_id,t.store_id,t.type,t.pay_time,t.city_code from (
	select
	    customer_id,torp.store_id,torp.type,torp.pay_time,ts.city_code
		ROW_NUMBER() OVER (partition BY customer_id ORDER BY create_time ASC) as rn
	from gemini.t_order_receipts torp
	left join t_store ts ON (ts.store_id = tbatch.store_id) 
	where torp.pay_status ='payed' and (torp.type = 'associator_start_2' OR torp.type = 'associator_up_2' OR torp.type='plus') 
) t where t.rn = 1
) torp ON (torp.customer_id = tc.customer_id)
LEFT JOIN (
	select  tcard.customer_id,tcard.type,tbatch.store_id,ts.city_code from (
		select tcard.customer_id,tcard.type,tbatch.store_id,ts.city_code
			ROW_NUMBER() OVER (partition BY customer_id ORDER BY create_time ASC) as rn
		from t_card tcard
		join t_exchange_card_batch tbatch on (tcard.batch_id = tbatch.id)
		left join t_store ts ON (ts.store_id = tbatch.store_id) where tcard.customer_id is not null 
	) t where t.rn = 1
) tcard ON (tcard.customer_id = tc.customer_id)
LEFT JOIN t_member_operation_record tmor ON (tomr.customer_id = tc.customer_id and torm.andmode ='adminDefined' and torm.level =2 )
where tc.associator_level = 2
and tc.white!='QA'



#款单表查询会员款单
select t.customer_id,t.store_id,t.type,t.pay_time,t.city_code from (
	select
	    customer_id,torp.store_id,torp.type,torp.pay_time,ts.city_code,
		ROW_NUMBER() OVER (partition BY torp.customer_id ORDER BY torp.create_time ASC) as rn
	from gemini.t_order_receipts torp
	left join gemini.t_store ts ON (ts.id = torp.store_id) 
	where torp.pay_status ='payed' and (torp.type = 'associator_start_2' OR torp.type = 'associator_up_2' OR torp.type='plus') 
) t where t.rn = 1

#集采用户查询
select  tcard.customer_id,tcard.type,tbatch.store_id from (
	select tcard.customer_id,tcard.type,tbatch.store_id,
		ROW_NUMBER() OVER (partition BY customer_id ORDER BY create_time ASC) as rn
	from t_card tcard
	join t_exchange_card_batch tbatch on (tcard.batch_id = tbatch.id) where tcard.customer_id is not null 
) t where t.rn = 1



#该表创建为视图：社员gmv 社员订单量，社员订单返利，社员订单退款，生日券使用次数，25元开卡券使用次数，是否领取开卡礼
select
	customer_id,
	sum(case when order_tag1 like '%M%' then trading_price else 0 end) as member_GMV,
	sum(case when order_tag1 like '%M%' then 1 else 0 end) as member_order_count,
	sum(case when order_tag1 like '%M%' then ifnull(apportion_rebate,0) else 0 end) as apportion_rebate,
	sum(case when order_tag1 like '%M%' then ifnull(returned_amount,0) else 0 end) as returned_amount,
	sum(case when order_tag4 = 'A2' then 1 else 0 end) as birthday_coupon_use_count,
	sum(case when order_tag4 = 'A1' then 1 else 0 end) as coupons_25_use_count,
	sum(case when order_tag4 = 'A3' then 1 else 0 end) as is_get_cardgift
from daqweb.df_mass_order_total GROUP BY customer_id





#依据df_user_member清洗历史社员数据
select dum.customer_id,
dum.mobilephone,
dum.idcard,
dum.birthplace,
dum.born_province,
dum.born_city,
dum.sex,
dum.regist_time,
dum.associator_level,
dum.customer_source,
tc.white as white,

dum.regist_storeid,
dum.regist_cityno,
dum.opencard_time,
dum.isnew_member,
dum.member_type,
dum.associator_expiry_date,
dum.invitecode,
1 as opencard_count,
dum.opencard_time as first_opencard_time,
dum.member_origin,

tor.member_GMV,
tor.member_order_count,
tor.apportion_rebate,
tor.birthday_coupon_use_count,
tor.coupons_25_use_count,
case when tor.is_get_cardgift != 0 then 1 else 0 end as is_get_cardgift,
case when torp.customer_id is not null then 1 else 0 end as regist_type,

dum.status,
now() as create_time,
now() as update_time
 from daqweb.df_user_member dum 
LEFT JOIN
(
select
	customer_id,
	sum(case when order_tag1 like '%M%' then trading_price else 0 end) as member_GMV,
	sum(case when order_tag1 like '%M%' then 1 else 0 end) as member_order_count,
	sum(case when order_tag1 like '%M%' then ifnull(apportion_rebate,0) else 0 end) as apportion_rebate,
	sum(case when order_tag1 like '%M%' then ifnull(returned_amount,0) else 0 end) as returned_amount,
	sum(case when order_tag4 = 'A2' then 1 else 0 end) as birthday_coupon_use_count,
	sum(case when order_tag4 = 'A1' then 1 else 0 end) as coupons_25_use_count,
	sum(case when order_tag4 = 'A3' then 1 else 0 end) as is_get_cardgift
	from daqweb.df_mass_order_total GROUP BY customer_id
) tor ON (tor.customer_id = dum.customer_id)
LEFT JOIN 
(
	select t.customer_id,t.store_id,t.type,t.pay_time,t.city_code from (
		select
		    customer_id,torp.store_id,torp.type,torp.pay_time,ts.city_code,
			ROW_NUMBER() OVER (partition BY torp.customer_id ORDER BY torp.create_time ASC) as rn
		from gemini.t_order_receipts torp
		left join gemini.t_store ts ON (ts.id = torp.store_id) 
		where torp.pay_status ='payed' and (torp.type = 'associator_start_2' OR torp.type = 'associator_up_2' OR torp.type='plus') 
	) t where t.rn = 1
) torp ON (torp.customer_id = dum.customer_id)
LEFT JOIN gemini.t_customer tc ON (tc.customer_id = dum.customer_id)





