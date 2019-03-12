SELECT 
CONCAT('\'',te.id) AS 'E店ID',
te.name AS 'E店名称',
ts.number AS '门店编码',
ts.name AS '门店名称',
tp.`code` AS'商品编码',
CONCAT('\'',tp.content_bar) AS '商品条码',
tp.content_name AS '商品名称',
tp.income_tax_rate AS '税率',
t_pvi.`code` AS '供应商编码',
t_pvi.`name` AS '供应商名称',
tiny.weighted_cost_price AS '商品加权成本价',
tiny.locked_number AS '商品库存数量（锁定）',
tiny.pro_number AS '商品库存数量（可售）',
tiny.pro_number*tiny.weighted_cost_price AS '金额'
FROM t_inventory tiny 
JOIN t_store ts ON ts.id=tiny.store_id AND (ts.city_code='024' OR ts.city_code='024') AND ts.status=0
JOIN t_eshop te ON tiny.eshop_id = te.id AND te.self_inventory='yes' AND te.status=0
JOIN t_product tp on tiny.pro_id=tp.id AND tp.status=0
JOIN t_provider t_pvi ON tp.provider_id = t_pvi.id
WHERE ts.white!='QA' AND tiny.`status`=0 
AND (tiny.locked_number>0 OR tiny.pro_number>0)
AND te.id in (
'd9d8248c225266322b8e21f8ae451add',
'a146dd0060ed1e5040996eaaed6fb0ec',
'4475de10c7e5810abd0e41977e2788e5',
'06293efe1b91995e696c1932f987753a')
ORDER BY te.id,ts.id




SELECT 
CONCAT('\'',te.id) AS 'E店ID',
te.name AS 'E店名称',
ts.number AS '门店编码',
ts.name AS '门店名称',
tp.`code` AS'商品编码',
CONCAT('\'',tp.content_bar) AS '商品条码',
tp.content_name AS '商品名称',
tp.income_tax_rate AS '税率',
t_pvi.`code` AS '供应商编码',
t_pvi.`name` AS '供应商名称',
tiny.weighted_cost_price AS '商品加权成本价',
tiny.locked_number AS '商品库存数量（锁定）',
tiny.pro_number AS '商品库存数量（可售）',
tiny.pro_number*tiny.weighted_cost_price AS '金额'
FROM t_inventory tiny 
JOIN t_store ts ON ts.id=tiny.store_id AND (ts.city_code='024' OR ts.city_code='024') 
JOIN t_eshop te ON tiny.eshop_id = te.id AND te.self_inventory='yes' AND te.status=0
JOIN t_product tp on tiny.pro_id=tp.id AND tp.status=0
JOIN t_provider t_pvi ON tp.provider_id = t_pvi.id
WHERE ts.white!='QA' AND tiny.`status`=0 
AND (tiny.locked_number>0 OR tiny.pro_number>0)
AND te.id in ('d9d8248c225266322b8e21f8ae451add',
'a146dd0060ed1e5040996eaaed6fb0ec',
'4475de10c7e5810abd0e41977e2788e5',
'06293efe1b91995e696c1932f987753a')
and tiny.warehouse_id in (
'60a6474a8df3471f8c4d21b0ce15fd7c',
'8ad889845f009679015f2e5e542909e6'
)
ORDER BY te.id,ts.id

