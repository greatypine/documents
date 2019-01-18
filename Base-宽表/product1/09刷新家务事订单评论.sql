
UPSERT into  gabase.b_item_pro_total 
(
	id,
	contents,
	star_level,
	rate,
	next_days,
	next_contents,
	star_level_1,
	star_level_2
)

select
bipt.id,
toc.contents,
cast(toc.star_level as TINYINT ) as star_level ,
toc.rate,
cast(toac.days as TINYINT ) AS next_days,
IFNULL(toac.contents, '') AS next_contents,
cast( toec.star_level_1 as TINYINT ) AS star_level_1,
cast( toec.star_level_2 as TINYINT ) AS star_level_2

from  gabase.b_item_pro_total  bipt 
left join 
(
	SELECT
		tocr.order_id,
		tocr.eshop_pro_id,
		max(tocr.contents) AS contents,
		max(tocr.star_level) AS star_level,
		max(tocr.rate) AS rate
	FROM
		gemini.t_order_comment tocr
	GROUP BY
		tocr.order_id,
		tocr.eshop_pro_id
)toc  on bipt.order_id = toc.order_id and bipt.eshop_pro_id = toc.eshop_pro_id
left join gemini.t_order_additional_comment toac on toac.order_id = bipt.order_id and bipt.eshop_pro_id = toac.eshop_pro_id
left join 
(
	SELECT
		toer.order_id,
		max(toer.star_level_1) AS star_level_1,
		max(toer.star_level_2) AS star_level_2
	FROM
		gemini.t_order_eshop_comment toer
	GROUP BY
		toer.order_id
)toec on toec.order_id = bipt.order_id

where 1=1
and bipt.department_id = '8ac29e835fed0a10015fed4d01dc0015' 
and bipt.sign_time > '2018-06' 
and bipt.sign_time < to_date(now())

