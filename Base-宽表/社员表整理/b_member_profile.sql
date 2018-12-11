

#该表创建为视图：社员gmv 社员订单量，社员订单返利，社员订单退款，生日券使用次数，25元开卡券使用次数，是否领取开卡礼
create view if not exsits v_member_consumption
select
	customer_id,
	sum(case when order_tag1 like '%M%' then trading_price else 0 end) as member_GMV,
	sum(case when order_tag1 like '%M%' then 1 else 0 end) as member_order_count,
	sum(case when order_tag1 like '%M%' then ifnull(apportion_rebate,0) else 0 end) as apportion_rebate,
	sum(case when order_tag1 like '%M%' then ifnull(returned_amount,0) else 0 end) as returned_amount,
	sum(case when order_tag4 = 'A2' then 1 else 0 end) as birthday_coupon_use_count,
	sum(case when order_tag4 = 'A1' then 1 else 0 end) as coupons_25_use_count,
	sum(case when order_tag4 = 'A3' then 1 else 0 end) as is_get_cardgift
from daqweb.df_mass_order_total where create_time < concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') GROUP BY customer_id





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
LEFT JOIN gemini.t_customer tc ON (tc.customer_id = dum.customer_id)

#更新
upsert INTO gabase.b_member_profile ( customer_id, regist_storeid, regist_cityno, opencard_time, associator_expiry_date, member_type, invitecode, opencard_count, MODE, status, update_time)
SELECT bmp.customer_id,
       t_tab.regist_storeid,
       t_tab.regist_cityno,
       t_tab.opencard_time,
       t_tab.associator_expiry_date,
       t_tab.member_type,
       t_tab.invitecode,
       case when 
        to_date(t_tab.opencard_time) != to_date(bmp.opencard_time) and to_date(t_tab.opencard_time) != to_date(bmp.first_opencard_time)
        then cast(bmp.opencard_count+1 as int) else cast(bmp.opencard_count as int) end as opencard_count,
       t_tab.MODE,
       t_tab.status,
       t_tab.update_time
FROM gabase.b_member_profile bmp
INNER JOIN
  ( SELECT tc.id AS customer_id,
           bca.opencard_store_id AS regist_storeid,
           bca.city_no AS regist_cityno,
           bca.create_time AS opencard_time,
           tc.associator_expiry_date AS associator_expiry_date,
           CASE
               WHEN torp.type IS NOT NULL
                    AND torp.type != '' THEN torp.type
               ELSE tcard.type
           END AS member_type,
           ext.invitecode AS invitecode,
           1 AS opencard_count,
           tmor.mode AS MODE,
           CASE
               WHEN tc.associator_expiry_date >now() THEN 0
               ELSE 1
           END AS status,
           cast(now() AS string) AS update_time
   FROM gemini.t_customer tc
   LEFT JOIN gabase.b_customer_associator bca ON (tc.id = bca.customer_id)
   LEFT JOIN gemini_mongo.t_customer_info_record_ext ext ON (ext.customerid = tc.id)
   LEFT JOIN
     ( SELECT t.customer_id,
              t.type
      FROM
        (SELECT customer_id,
                TYPE,
                ROW_NUMBER() OVER (partition BY customer_id
                                   ORDER BY create_time DESC) AS rn
         FROM gemini.t_order_receipts
         WHERE pay_status ='payed'
           AND (TYPE = 'associator_start_2'
                OR TYPE = 'associator_up_2'
                OR TYPE='plus') ) t
      WHERE t.rn = 1 ) torp ON (torp.customer_id = tc.id)
   LEFT JOIN
     ( SELECT customer_id,
              MODE
      FROM
        ( SELECT customer_id,
                 MODE,
                 ROW_NUMBER() OVER (partition BY customer_id
                                    ORDER BY create_time DESC) AS rn
         FROM gemini.t_member_operation_record
         WHERE LEVEL = 2 ) ta
      WHERE ta.rn = 1 ) tmor ON (tmor.customer_id = tc.id)
   LEFT JOIN
     ( SELECT t.customer_id,
              t.type
      FROM
        (SELECT tcard.customer_id,
                tcard.type,
                ROW_NUMBER() OVER (partition BY customer_id
                                   ORDER BY tcard.create_time DESC) AS rn
         FROM gemini.t_card tcard
         JOIN gemini.t_exchange_card_batch tbatch ON (tcard.batch_id = tbatch.id)
         WHERE tcard.customer_id IS NOT NULL ) t
      WHERE t.rn = 1 ) tcard ON (tcard.customer_id = tc.id)
   WHERE ((tc.create_time >=concat(from_unixtime(unix_timestamp(date_sub(now(), 1)),"yyyy-MM-dd"), ' 00:00:00')
           AND tc.create_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00'))
          OR (tc.update_time >=concat(from_unixtime(unix_timestamp(date_sub(now(), 1)),"yyyy-MM-dd"), ' 00:00:00')
              AND tc.update_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')))
     AND tc.associator_level = 2
     AND tc.white!='QA' ) t_tab ON (bmp.customer_id = t_tab.customer_id);

#增量
insert INTO gabase.b_member_profile 
SELECT tc.id AS customer_id,
        tc.mobilephone,
        bca.idcard,
        case when bca.idcard is not null and bca.idcard != '' then strleft(bca.idcard,6) else null end 
        as birthplace,
        case when bca.idcard is not null and bca.idcard != '' then strleft(bca.idcard,2) else null end 
        as born_province,
        case when bca.idcard is not null and bca.idcard != '' then strleft(bca.idcard,4) else null end 
        as born_city,
        case when bca.idcard is not null and bca.idcard != '' then substr(bca.idcard,)(bca.idcard,4) else null end 
        as born_city,
        tca.birthday as birthday,
           bca.opencard_store_id AS regist_storeid,
           bca.city_no AS regist_cityno,
           bca.create_time AS opencard_time,
           tc.associator_expiry_date AS associator_expiry_date,
           CASE
               WHEN torp.type IS NOT NULL
                    AND torp.type != '' THEN torp.type
               ELSE tcard.type
           END AS member_type,
           ext.invitecode AS invitecode,
           1 AS opencard_count,
           tmor.mode AS MODE,
           CASE
               WHEN tc.associator_expiry_date >now() THEN 0
               ELSE 1
           END AS status,
           cast(now() AS string) AS update_time
   FROM gemini.t_customer tc
   LEFT JOIN gabase.b_customer_associator bca ON (tc.id = bca.customer_id)
   LEFT JOIN gemini_mongo.t_customer_info_record_ext ext ON (ext.customerid = tc.id)
   LEFT JOIN
     ( SELECT t.customer_id,
              t.type
      FROM
        (SELECT customer_id,
                TYPE,
                ROW_NUMBER() OVER (partition BY customer_id
                                   ORDER BY create_time DESC) AS rn
         FROM gemini.t_order_receipts
         WHERE pay_status ='payed'
           AND (TYPE = 'associator_start_2'
                OR TYPE = 'associator_up_2'
                OR TYPE='plus') ) t
      WHERE t.rn = 1 ) torp ON (torp.customer_id = tc.id)
   LEFT JOIN
     ( SELECT customer_id,
              MODE
      FROM
        ( SELECT customer_id,
                 MODE,
                 ROW_NUMBER() OVER (partition BY customer_id
                                    ORDER BY create_time DESC) AS rn
         FROM gemini.t_member_operation_record
         WHERE LEVEL = 2 ) ta
      WHERE ta.rn = 1 ) tmor ON (tmor.customer_id = tc.id)
   LEFT JOIN
     ( SELECT t.customer_id,
              t.type
      FROM
        (SELECT tcard.customer_id,
                tcard.type,
                ROW_NUMBER() OVER (partition BY customer_id
                                   ORDER BY tcard.create_time DESC) AS rn
         FROM gemini.t_card tcard
         JOIN gemini.t_exchange_card_batch tbatch ON (tcard.batch_id = tbatch.id)
         WHERE tcard.customer_id IS NOT NULL ) t
      WHERE t.rn = 1 ) tcard ON (tcard.customer_id = tc.id)
   WHERE ((tc.create_time >=concat(from_unixtime(unix_timestamp(date_sub(now(), 1)),"yyyy-MM-dd"), ' 00:00:00')
           AND tc.create_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00'))
          OR (tc.update_time >=concat(from_unixtime(unix_timestamp(date_sub(now(), 1)),"yyyy-MM-dd"), ' 00:00:00')
              AND tc.update_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')))
     AND tc.associator_level = 2
     AND tc.white!='QA'







create table if not exists gabase.b_member_profile(
	   customer_id string,
       mobilephone string,
       idcard string,
       birthplace string,
       born_province string,
       born_city string,
       birthday string,
       sex string,
       regist_time string,
       associator_level int,
       customer_source string,
       white string,
       regist_storeid string,
       regist_cityno string,
       opencard_time string,
       associator_expiry_date string,
       isnew_member int,
       member_type string,
       invitecode string,
       opencard_count int,
       first_opencard_time string,
       member_origin string,
       mode string,
       member_GMV double,
       member_order_count int,
       apportion_rebate double,
       birthday_coupon_use_count int,
       coupons_25_use_count int,
       is_get_cardgift int,
       status int,
       create_time string,
       update_time string
)

