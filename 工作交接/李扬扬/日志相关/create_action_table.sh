#!/bin/bash

impala-shell -q "invalidate metadata"

#往前推的天数,1 就是昨天
data_param="1"

#api

impala-shell -q "drop table gemini.log_api_sec"

impala-shell -q "create table gemini.log_api_sec as 
 select la.* , ifnull(la.phone , er.mobilephone)  as sec_phone , er.id  as customer_id, er.rolename  
 from gemini.log_api  la  ,gemini.view_employee_role er  
 where  la.token = er.app_token 
 and er.app_token != ''
 and la.token != ''
 and la.token is not null 
 and er.app_token is not null 
 and  la.createdate = from_unixtime(unix_timestamp(days_sub(now(), ${data_param})), 'yyyy-MM-dd')
 and la.requesturi  in (
 '/gasq-web-thirdparty/pay/multiPay',
 '/gasq-web-customer/customer/smslogin',
 '/gasq-web-order/order/comment',
 '/gasq-web-thirdparty/selfService/updateGroupOrderInfo',
 '/gasq-web-order/order/employee/start',
 '/gasq-web-thirdparty/sms/sendSms',
 '/gasq-web-thirdparty/payment/charge',
 '/gasq-web-thirdparty/payment/charge/associator',
 '/gasq-web-order/order/placeorders',
 '/gasq-web-customer/customer/logout'
)"

#关联t_customer 

impala-shell -q "insert into gemini.log_api_sec
select la.* , ifnull(la.phone ,   tc.mobilephone)  as sec_phone  , tc.id as customer_id , null as rolename 
from gemini.log_api  la  ,  gemini.t_customer tc 
where  la.token = tc.app_token 
and tc.app_token != ''
and la.token != ''
and la.token is not null 
and tc.app_token is not null 
and  la.createdate = from_unixtime(unix_timestamp(days_sub(now(), ${data_param})), 'yyyy-MM-dd')
and la.requesturi  in (
'/gasq-web-thirdparty/pay/multiPay',
'/gasq-web-customer/customer/smslogin',
'/gasq-web-order/order/comment',
'/gasq-web-thirdparty/selfService/updateGroupOrderInfo',
'/gasq-web-order/order/employee/start',
'/gasq-web-thirdparty/sms/sendSms',
'/gasq-web-thirdparty/payment/charge',
'/gasq-web-thirdparty/payment/charge/associator',
'/gasq-web-order/order/placeorders',
'/gasq-web-customer/customer/logout'
)"

#关联groupid和orderid补充customerid

impala-shell -q "
insert into gemini.log_api_sec
select
la.adtag,
la.apptypeplatform, 
la.createdate,
la.createtime, 
null as employee_role,
ifnull(la.group_id,tor.group_id) as group_id ,
la.id, 
la.mdop ,
la.message, 
la.order_id as order_id  ,
la.payplatform ,
tc.mobilephone as phone ,
la.requesturi,
la.store_id,
la.token,
ifnull(tc.mobilephone,la.phone) as sec_phone  ,
ifnull(tor.customer_id , tog.customer_id ) as customer_id ,
null as rolename 
from gemini.log_api la 
left join gemini.t_order tor on la.order_id = tor.id 
left join gemini.t_order_group tog  on  la.group_id = tog.id 
left join gemini.t_customer tc on ifnull(tor.customer_id , tog.customer_id ) = tc.id 
where la.token is not null 
and la.token != ''
and la.token not in (select token from gemini.log_api_sec)
and (la.order_id is not null or la.group_id is not null )
and  la.createdate = from_unixtime(unix_timestamp(days_sub(now(), ${data_param})), 'yyyy-MM-dd')
and la.requesturi  in (
'/gasq-web-thirdparty/pay/multiPay',
'/gasq-web-customer/customer/smslogin',
'/gasq-web-order/order/comment',
'/gasq-web-thirdparty/selfService/updateGroupOrderInfo',
'/gasq-web-order/order/employee/start',
'/gasq-web-thirdparty/sms/sendSms',
'/gasq-web-thirdparty/payment/charge',
'/gasq-web-thirdparty/payment/charge/associator',
'/gasq-web-order/order/placeorders',
'/gasq-web-customer/customer/logout'
)"
  
#log_api 添加到collect
hive -e "insert into gemini.log_collect 
select 
lam.requesturi as behavior ,
bbn.behavior_name,
null as behavior_param,
lam.createtime as create_date,
lam.customer_id,
lam.rolename as employee_role,
null as eshop_id,
if(lam.group_id is not null ,lam.group_id ,vog1.group_id ) as group_id,
lam.id,
'api' as log_type,
lam.message,
if(lam.order_id is not null , lam.order_id  ,vog2.order_id ) as order_id,
lam.payplatform ,
lam.sec_phone ,
null as product_id,
from_unixtime(unix_timestamp(lam.createtime), 'yyyy-MM-dd') as simple_date,
lam.store_id as store_id,
lam.token,
lam.apptypeplatform  as two_behavior ,
lam.mdop  as two_behavior_name
from gemini.log_api_sec lam
left join gemini.view_order_group vog1  on lam.order_id = vog1.order_id 
left join gemini.view_order_group vog2 on lam.group_id = vog2.group_id
left join datacube.b_behavior_name bbn on lam.requesturi = bbn.behavior;
"

impala-shell -q "refresh gemini.log_collect"

#log_eshop

impala-shell -q "drop table gemini.customer_mac"

impala-shell -q "CREATE TABLE gemini.customer_mac AS
SELECT customer_id,
       mac_address
FROM
  (SELECT ROW_NUMBER() OVER(PARTITION BY mac_address
                            ORDER BY create_date DESC) rn,
                       t2.customer_id,
                       t2.mac_address
   FROM gemini.log_eshop_two t2
   WHERE t2.mac_address IS NOT NULL
     AND t2.customer_id IS NOT NULL
     AND t2.create_date > from_unixtime(unix_timestamp(days_sub(now(), 30)), 'yyyy-MM-dd') ) t1
WHERE t1.rn = 1 "

#mac 地址补充customerid

##################修改

#impala-shell -q "drop table gemini.log_eshop"
impala-shell -q "
insert into    table    gemini.log_eshop   
select    
leo.create_date,
if(leo.customer_id is null , cm.customer_id,leo.customer_id) as customer_id,
leo.equipment_type,
leo.eshop_id,
leo.exh,
leo.id,
leo.ip,
leo.jc,
leo.jt ,
leo.mac_address,
leo.opean_id ,
leo.order_id ,
leo.phone,
leo.store_id,
leo.product_id,
leo.task,
leo.type,
leo.type_id,
from_unixtime(unix_timestamp(leo.create_date), 'yyyy-MM-dd') as simple_date
from gemini.log_eshop_two leo 
left join gemini.customer_mac cm  on leo.mac_address  = cm.mac_address 
where from_unixtime(unix_timestamp(leo.create_date), 'yyyy-MM-dd')  =  
from_unixtime(unix_timestamp(days_sub(now(), ${data_param})), 'yyyy-MM-dd');
"
#eshop日志加入collect
hive -e "
insert into gemini.log_collect 
select 
lep.type_id as behavior,
bbn.behavior_name,
lep.task as  behavior_param,
lep.create_date,
lep.customer_id,
erv.rolename as employee_role,
lep.eshop_id ,
tor.group_id ,
lep.id,
'eshop' as log_type,
null as message,
lep.order_id,
null as payPlatform,
if(lep.phone is not null ,lep.phone , tcr.mobilephone) as phone,
lep.product_id,
from_unixtime(unix_timestamp(lep.create_date), 'yyyy-MM-dd') as simple_date,
lep.store_id,
null as token ,
lep.jc as two_behavior,
lep.jt as two_behavior_name
from 
gemini.log_eshop lep
left join gemini.t_order  tor  on lep.order_id = tor.id 
left join gemini.t_customer tcr on tor.customer_id = tcr.id 
left join gemini.view_employee_role erv on if(lep.phone is null ,lep.phone , tcr.mobilephone)  = erv.mobilephone
left join datacube.b_behavior_name bbn on lep.task = bbn.behavior
where from_unixtime(unix_timestamp(lep.create_date), 'yyyy-MM-dd')  =  
date_sub(CURRENT_DATE,${data_param}) ;
"
impala-shell -q "refresh gemini.log_collect"

#生成final表

impala-shell -q "drop table  gemini.groupid_type" 

impala-shell -q " CREATE TABLE gemini.groupid_type AS
SELECT type,
       order_group_id
FROM
  (SELECT ROW_NUMBER() OVER(PARTITION BY order_group_id
                            ORDER BY create_time DESC) rn,
                       t2.type,
                       t2.order_group_id
   FROM gemini.t_order_receipts t2
   WHERE t2.order_group_id IS NOT NULL
     AND t2.type IS NOT NULL) t1
WHERE t1.rn = 1 "


##################修改

impala-shell -q "drop table gemini.log_final_one"

impala-shell -q "create table gemini.log_final_one  as 
select 
lct.*,
--t_order_item 
toi.id  as order_item_id,
toi.sku_rule_id ,
ifnull(lct.product_id,toi.eshop_pro_id) as pro_id,
--t_order_item_extra
toie.sum_trading_price,
toie.apportion_score,
toie.apportion_coupon,
--t_order
tor.total_quantity,
tor.score,
tor.order_type,
tor.payable_price,
tor.groupon_instance_id,
tor.order_source,
--t_order_group
tog.card_coupon_id,
--t_card_coupon
tcc.type_id,
tcc.content_price,
--t_order_receipts
trs.type as receipt_type,
--t_store
store.city_code ,
store.province_code,
--t_eshop
tep.eshop_kind as eshop_kind,
tep.self as eshop_self,
tep.type as eshop_type,
tep.area_company_id as eshop_area_company_id,
tep.bussiness_group_id,
tep.channel_id
from gemini.log_collect lct 
left join gemini.t_order_item toi on lct.order_id = toi.order_id 
left join gemini.t_order_item_extra toie on toi.id = toie.order_item_id
left join gemini.t_order tor on lct.order_id = tor.id 
left join gemini.t_order_group tog on lct.group_id= tog.id 
left join gemini.t_card_coupon tcc on tog.card_coupon_id = tcc.id 
left join gemini.groupid_type trs on lct.group_id = trs.order_group_id
left join gemini.t_store store on lct.store_id = store.id 
left join gemini.t_eshop tep on lct.eshop_id = tep.id 
 "
 
 impala-shell -q "drop table gemini.log_final_two"
 
 impala-shell -q "create table gemini.log_final_two  as 
 
select 
lfo.behavior ,                              
lfo.behavior_name ,                         
lfo.behavior_param ,                        
lfo.create_date ,                           
lfo.customer_id ,                           
lfo.employee_role ,                         
lfo.eshop_id ,                              
lfo.group_id ,                              
lfo.id ,                                    
lfo.log_type ,                              
lfo.message ,                               
lfo.order_id ,                              
lfo.payplatform ,                           
lfo.phone ,                                 
lfo.pro_id as product_id ,                            
lfo.simple_date ,                           
lfo.store_id ,                              
lfo.token ,                                 
lfo.two_behavior ,                          
lfo.two_behavior_name ,                     
lfo.order_item_id ,                         
lfo.sku_rule_id ,                                                  
lfo.sum_trading_price ,                     
lfo.apportion_score ,                       
lfo.apportion_coupon ,                      
lfo.total_quantity ,                           
lfo.score ,                                    
lfo.order_type ,                            
lfo.payable_price ,                         
lfo.groupon_instance_id ,                   
lfo.order_source ,                          
lfo.card_coupon_id ,                        
lfo.type_id ,                               
lfo.content_price ,                         
lfo.receipt_type ,                          
lfo.city_code ,                             
lfo.province_code ,                         
lfo.eshop_kind ,                            
lfo.eshop_self ,                            
lfo.eshop_type ,                            
lfo.eshop_area_company_id ,                 
lfo.bussiness_group_id ,                    
lfo.channel_id ,
tpt.cost_price  as product_cost_price ,
tpt.provider_id  as product_provider_id ,
tpt.is_support_use_rebates  as product_is_support_use_rebates,
tpt.credit  as product_credit,
tpt.rebates_mode  as product_rebates_mode,
tpt.content_name as product_name

from gemini.log_final_one lfo 
left join  gemini.t_product tpt on lfo.pro_id = tpt.id 
 "


#补充 签收,取消

impala-shell -q "
insert into gemini.log_final_two
select 
tor.order_status as  behavior ,
if(tor.order_status= 'signed' , '签收订单', '取消订单') as behavior_name ,
null as behavior_param ,
tor.create_time as create_date,
tor.create_user_id as customer_id,
null as  employee_role ,
tor.eshop_id ,
tor.group_id ,
REGEXP_REPLACE(uuid(), '-','') as id,
tor.order_status as log_type ,
tor.abnormal_remark as  message ,
tor.id as  order_id ,
null as payplatform ,
tc.mobilephone as  phone ,
tpt.id as product_id,
from_unixtime(unix_timestamp(tor.create_time), 'yyyy-MM-dd')    as simple_date,
tor.store_id,
null as token ,
null as  two_behavior ,
null as  two_behavior_name ,
toi.id as  order_item_id ,
toi.sku_rule_id ,
toie.sum_trading_price ,
toie.apportion_score ,
toie.apportion_coupon ,
tor.total_quantity ,
tor.score ,
tor.order_type ,
tor.payable_price ,
tor.groupon_instance_id ,
tor.order_source ,
tog.card_coupon_id ,
tcc.type_id,
tcc.content_price,
trs.type as receipt_type,
tse.city_code ,
tse.province_code ,
tep.eshop_kind as eshop_kind,
tep.self as eshop_self,
tep.type as eshop_type,
tep.area_company_id as eshop_area_company_id,
tep.bussiness_group_id,
tep.channel_id,
tpt.cost_price  as product_cost_price ,
tpt.provider_id  as product_provider_id ,
tpt.is_support_use_rebates  as product_is_support_use_rebates,
tpt.credit  as product_credit,
tpt.rebates_mode  as product_rebates_mode,
tpt.content_name as product_name
from gemini.t_order tor 
left join gemini.t_order_item toi on  tor.id = toi.order_id 
left join gemini.t_order_item_extra toie on toi.id = toie.order_item_id
left join gemini.t_order_group tog on tor.group_id = tog.id
left join gemini.t_card_coupon tcc on  tog.card_coupon_id = tcc.id 
left join gemini.groupid_type trs on tor.group_id = trs.order_group_id
left join gemini.t_product tpt on toi.eshop_pro_id = tpt.id 
left join gemini.t_customer tc on tor.create_user_id = tc.id 
left join gemini.t_store tse on tor.store_id = tse.id 
left join gemini.t_eshop tep on tor.eshop_id = tep.id 
where tor.order_status in('signed','cancel') 
and from_unixtime(unix_timestamp(tor.create_time ), 'yyyy-MM-dd') = 
from_unixtime(unix_timestamp(days_sub(now(), ${data_param})), 'yyyy-MM-dd')"

#退货

impala-shell -q "
insert into gemini.log_final_two
select 
'refund' as  behavior ,
'退货' as behavior_name ,
null as behavior_param ,
tord.create_time as create_date,
tor.create_user_id as customer_id,
null as employee_role ,
tord.eshop_id ,
tord.order_group_id as group_id ,
REGEXP_REPLACE(uuid(), '-','') as  id,
'refund' as log_type ,
tord.remark as  message ,
vog.order_id ,
null as payplatform ,
tc.mobilephone as  phone ,
tpt.id as product_id,
from_unixtime(unix_timestamp(tord.create_time), 'yyyy-MM-dd')    as simple_date,
tord.store_id,
null as token ,
null as two_behavior ,
null as two_behavior_name,
toi.id as order_item_id,
toi.sku_rule_id,
toie.sum_trading_price,
toie.apportion_score,
toie.apportion_coupon , 
tor.total_quantity ,
tor.score ,
tor.order_type ,
tor.payable_price ,
tor.groupon_instance_id ,
tor.order_source ,
tog.card_coupon_id ,
tcc.type_id,
tcc.content_price,
trs.type as receipt_type,
tse.city_code ,
tse.province_code ,
tep.eshop_kind as eshop_kind,
tep.self as eshop_self,
tep.type as eshop_type,
tep.area_company_id as eshop_area_company_id,
tep.bussiness_group_id,
tep.channel_id,
tpt.cost_price  as product_cost_price ,
tpt.provider_id  as product_provider_id ,
tpt.is_support_use_rebates  as product_is_support_use_rebates,
tpt.credit  as product_credit,
tpt.rebates_mode  as product_rebates_mode,
tpt.content_name as product_name
from gemini.t_order_refund tord 
left join gemini.view_order_group vog on tord.order_group_id  = vog.group_id
left join gemini.t_order_item toi on vog.order_id = toi.order_id 
left join gemini.t_order_item_extra toie on toi.id = toie.order_item_id
left join gemini.t_order tor on toi.order_id = tor.id 
left join gemini.t_order_group tog on tord.order_group_id = tog.id
left join gemini.t_card_coupon tcc on tog.card_coupon_id = tcc.id 
left join gemini.groupid_type trs on tord.order_group_id = trs.order_group_id
left join gemini.t_product tpt on toi.eshop_pro_id = tpt.id 
left join gemini.t_customer tc on tor.create_user_id = tc.id 
left join gemini.t_store tse on tord.store_id = tse.id 
left join gemini.t_eshop tep on tord.eshop_id = tep.id 
where from_unixtime(unix_timestamp(tord.create_time ), 'yyyy-MM-dd') = 
from_unixtime(unix_timestamp(days_sub(now(), ${data_param})), 'yyyy-MM-dd')"

#款单
 impala-shell -q "
 
insert into gemini.log_final_two
select 
'receipts' as behavior ,
'款单' as behavior_name ,
null as behavior_param ,
tors.create_time as create_date,
tors.customer_id,
null as employee_role ,
tors.eshop_id,
tors.order_group_id as group_id,
REGEXP_REPLACE(uuid(), '-','') as  id,
'receipts' as log_type ,
null as message ,
vog.order_id,
tors.pay_platform as payplatform ,
tc.mobilephone as  phone ,
tpt.id as product_id,
from_unixtime(unix_timestamp(tors.create_time), 'yyyy-MM-dd')    as simple_date,
tors.store_id,
null as token ,
null as two_behavior ,
null as two_behavior_name ,
toi.id as order_item_id,
toi.sku_rule_id,
toie.sum_trading_price,
toie.apportion_score,
toie.apportion_coupon, 
tor.total_quantity ,
tor.score ,
tor.order_type ,
tor.payable_price ,
tor.groupon_instance_id ,
tor.order_source ,
tog.card_coupon_id ,
tcc.type_id,
tcc.content_price,
tors.type as  receipt_type ,
tse.city_code ,
tse.province_code ,
tep.eshop_kind as eshop_kind,
tep.self as eshop_self,
tep.type as eshop_type,
tep.area_company_id as eshop_area_company_id,
tep.bussiness_group_id,
tep.channel_id,
tpt.cost_price  as product_cost_price ,
tpt.provider_id  as product_provider_id ,
tpt.is_support_use_rebates  as product_is_support_use_rebates,
tpt.credit  as product_credit,
tpt.rebates_mode  as product_rebates_mode,
tpt.content_name as product_name
from gemini.t_order_receipts tors 
left join gemini.view_order_group vog on tors.order_group_id  = vog.group_id
left join gemini.t_order_item toi on vog.order_id = toi.order_id 
left join gemini.t_order_item_extra toie on toi.id = toie.order_item_id
left join gemini.t_order tor on toi.order_id = tor.id 
left join gemini.t_order_group tog on tors.order_group_id = tog.id
left join gemini.t_card_coupon tcc on tog.card_coupon_id = tcc.id 
left join gemini.t_product tpt on toi.eshop_pro_id = tpt.id 
left join gemini.t_customer tc on tors.customer_id = tc.id 
left join gemini.t_store tse on tors.store_id = tse.id 
left join gemini.t_eshop tep on tors.eshop_id = tep.id 
where from_unixtime(unix_timestamp(tors.create_time), 'yyyy-MM-dd') =
from_unixtime(unix_timestamp(days_sub(now(), ${data_param})), 'yyyy-MM-dd')"

# 搜索

impala-shell -q "
insert into gemini.log_final_two
select
'search' as behavior ,
'搜索' as behavior_name ,
ls.key as behavior_param ,
ls.create_time as create_date,
ls.customer_id,
ver.rolename as employee_role ,
null as eshop_id,
null as group_id,
REGEXP_REPLACE(uuid(), '-','') as  id,
'search' as log_type ,
null as message ,
null as order_id,
null as payplatform ,
ls.mobilephone as  phone ,
ls.product_id,
ls.simple_date,
ls.store_id,
null as token ,
null as two_behavior ,
null as two_behavior_name ,
null as order_item_id,
null as sku_rule_id,
null as sum_trading_price,
null as apportion_score,
null as apportion_coupon,
null as total_quantity ,
null as score ,
null as order_type ,
null as payable_price ,
null as groupon_instance_id ,
null as order_source ,
null as card_coupon_id ,
null as type_id,
null as content_price,
null as receipt_type ,
null as city_code ,
null as province_code ,
null as eshop_kind,
null as eshop_self,
null as eshop_type,
null as eshop_area_company_id,
null as bussiness_group_id,
null as channel_id,
null as product_cost_price ,
null as product_provider_id ,
null as product_is_support_use_rebates,
null as product_credit,
null as product_rebates_mode,
ls.product_name
from gemini.log_search ls
left join  gemini.view_employee_role ver on ls.customer_id =  ver.id
where ls.simple_date =
from_unixtime(unix_timestamp(days_sub(now(), ${data_param})), 'yyyy-MM-dd')"


########


#impala-shell -q "drop table  datacube_kudu.log_final"
impala-shell -q "insert into  table   datacube_kudu.log_final  
select 
 lss.*,fnv_hash(concat_ws(lss.create_date , lss.behavior_name, lss.customer_id)) as action_id
 from gemini.log_final_two lss "

impala-shell -q "Compute Stats datacube_kudu.log_final"
