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
JOIN t_store ts ON ts.id=tiny.store_id AND (ts.city_code='0311') AND ts.status=0
JOIN t_eshop te ON tiny.eshop_id = te.id AND te.self_inventory='yes' AND te.status=0
JOIN t_product tp on tiny.pro_id=tp.id AND tp.status=0
JOIN t_provider t_pvi ON tp.provider_id = t_pvi.id
WHERE ts.white!='QA' AND tiny.`status`=0 
AND (tiny.locked_number>0 OR tiny.pro_number>0)
AND te.id in (
'00748da4b21827ab2a3922aa6622c080',
'dd6ade808c5704dbfb1ec49ff4371cf2')
ORDER BY te.id,ts.id





