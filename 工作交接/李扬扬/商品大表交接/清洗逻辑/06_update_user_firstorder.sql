UPSERT into  gabase.b_item_pro_total 
(
	id,
	order_id,
	usertag1
)
select 
	bipt.id,
	bipt.order_id,
	case WHEN uptor.numberOid=1 then 'yes' ELSE 'no' END  
from  gabase.b_item_pro_total  bipt
INNER JOIN ( 
		SELECT
		tor.customer_id ,
		min(tor.create_time) AS  max_time,
		COUNT(DISTINCT tor.id) as numberOid
		FROM
		 gemini.t_order   tor 
		GROUP BY tor.customer_id  
) uptor  ON  bipt.create_time=uptor.max_time AND bipt.customer_id=uptor.customer_id
WHERE  bipt.create_time<to_date(now())



