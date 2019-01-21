select s.gb_code,s.name,t.* from t_store t
left join  t_town  s on t.town_id=s.id
where  1=1
AND t.name NOT LIKE '%测试%'
AND t.name NOT LIKE '%企业购%' 
AND t.name NOT LIKE '%内购%' 
AND t.name NOT LIKE '%校园%' 
and t.name NOT LIKE '%停用%' 
and t.name NOT LIKE '%仓储%'
AND t.name NOT LIKE '%云门店%' 
AND t.name NOT LIKE '%前置仓%'
AND t.name NOT LIKE '%爱科%'
AND t.name NOT LIKE '%大学%'
AND t.name NOT LIKE '%仓店%'
AND t.name NOT LIKE '%电视端%'
AND t.city_code in ('010','022','021','024','020','0851','0871')
AND t.id in 
(select store_id  from  t_v_full_order a 
where 1=1
and a.order_create_time<now()
and a.order_create_time>=date_sub(now(),90)
group by a.store_id having count(distinct a.customer_id)>=100
)