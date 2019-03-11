UPSERT INTO gabase.b_item_pro_total 
(
	id,
	unit_price,
	quantity,
	order_status
)
SELECT uptor.id,
       gemini_online.unit_price,
       gemini_online.quantity,
       gemini_online.order_status
FROM gabase.b_item_pro_total  uptor
INNER JOIN
  (SELECT toi.id AS item_id,
          IF(tor.is_split='yes',toi.original_price,toi.unit_price) AS unit_price,
          toi.quantity AS quantity,
          tor.order_status
   FROM gemini.t_order_item toi
   JOIN gemini.t_order tor ON tor.id=toi.order_id
   WHERE tor.update_time>= to_date(date_sub(now(),1)) 
   AND tor.update_time< to_date(now())  ) gemini_online ON gemini_online.item_id=uptor.id
