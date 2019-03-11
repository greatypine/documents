UPSERT into  gabase.b_item_pro_total 
(
	id,
	order_id,
        success_time,
        sign_time,
	cancel_time,
        commented_time
) 
select 
 bipt.id,
 bipt.order_id,
 upbipt.success_time,
 upbipt.signed_time,
 upbipt.cancel_time,
 upbipt.commented_time 
from gabase.b_item_pro_total  bipt  INNER JOIN 
(
	SELECT
		t.id,
		MAX(t.success_time) AS    success_time,
		MAX(t.signed_time) AS 	  signed_time,
		max(t.cancel_time) AS  cancel_time,
		MAX(t.commented_time) AS  commented_time
	FROM
		(
			SELECT tor.id,
				IF(tof.order_status='signed', tof.create_time, NULL ) AS signed_time,
				IF(tof.order_status='success', tof.create_time, NULL ) AS success_time,
				IF(tof.order_status='cancel', tof.create_time, NULL ) AS cancel_time,
				IF(tof.order_status='commented', tof.create_time, NULL ) AS commented_time
			FROM gemini.t_order_flow tof  
			JOIN gemini.t_order tor ON tor.id=tof.order_id
			 WHERE (tof.order_status='signed' OR tof.order_status='success' OR tof.order_status='commented'  OR tof.order_status='cancel'  )
			 and ( tof.create_time > '2018-06' or tor.update_time > '2018-06'  )
			 and ( tof.create_time < to_date(now()) or  tor.update_time < to_date(now()) )
		) t
	GROUP BY t.id


) upbipt  ON  bipt.order_id=upbipt.id
where bipt.department_id = '8ac29e835fed0a10015fed4d01dc0015'



