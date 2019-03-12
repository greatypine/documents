 
UPSERT into  gabase.b_item_pro_total 
(
	id,
	order_profit
)
select
bipt.id,
gabase.item_profit(
	bipt.trading_price ,
	bipt.quantity,
	bipt.joint_ims,
	ifnull(bipt.order_tag3 , ''),
	bipt.teptag1,
	bipt.business_type,
	bipt.cost_price ,
	ifnull(bipt.c_cost_price , 0.0) ,
	ifnull(bipt.contract_method , 'no') ,
	ifnull(bipt.contract_percent , 0) ,
	ifnull(bipt.contract_price , 0) ,
	ifnull(bipt.proration_seller_price , 0) 
) as order_profit

from  gabase.b_item_pro_total  bipt 
WHERE 1=1 
 and bipt.sign_time>= to_date(date_sub(now(),3)) 
 and bipt.sign_time< to_date(now()) 
