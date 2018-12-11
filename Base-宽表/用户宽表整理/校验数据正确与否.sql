--1 b_user_profile
--验证数据1.数据查漏

SELECT *
FROM
  (SELECT customer_id
   FROM gemini.t_order
   WHERE sign_time IS NOT NULL
     AND sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')
   GROUP BY customer_id) b
WHERE b.customer_id NOT IN
    (SELECT customer_id
     FROM b_user_profile);

--校验数据准确性

SELECT *
FROM b_user_profile
LIMIT 1;

--1.校验order_sn_max,trading_price_max

SELECT *
FROM
  (SELECT b.customer_id,
          b.trading_price_max,
          c.trading_price
   FROM b_user_profile b
   INNER JOIN
     (SELECT customer_id,
             max(trading_price) AS trading_price
      FROM gemini.t_order tor
      INNER JOIN gemini.t_eshop te ON (tor.eshop_id = te.id)
      WHERE te.`name` NOT LIKE '%测试%'
        AND te.white!='QA'
        AND tor.sign_time IS NOT NULL
        AND tor.sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')
      GROUP BY customer_id) c ON (b.customer_id = c.customer_id)) d
WHERE d.trading_price_max != d.trading_price;

select * from (
SELECT e.customer_id,
       f.order_sn,
       e.order_sn_max
FROM b_user_profile e
INNER JOIN
  (SELECT a.customer_id,
          max(a.order_sn) AS order_sn
   FROM
     (SELECT tor.customer_id,
             tor.order_sn,
             tor.trading_price
      FROM gemini.t_order tor
      INNER JOIN gemini.t_eshop te ON (tor.eshop_id = te.id)
      WHERE te.`name` NOT LIKE '%测试%'
        AND te.white!='QA'
        AND tor.sign_time IS NOT NULL
        AND tor.sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') ) a
   INNER JOIN
     ( SELECT customer_id,
              max(trading_price) AS trading_price
      FROM gemini.t_order tor
      INNER JOIN gemini.t_eshop te ON (tor.eshop_id = te.id)
      WHERE te.`name` NOT LIKE '%测试%'
        AND te.white!='QA'
        AND tor.sign_time IS NOT NULL
        AND tor.sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')
      GROUP BY customer_id) d ON (a.customer_id = d.customer_id
                                  AND a.trading_price = d.trading_price)
   GROUP BY a.customer_id) f ON (e.customer_id = f.customer_id) ) g where g.order_sn != g.order_sn_max;
   
   
-- 查询末次消费信息  末次订单编号，产品数量，金额有差异，差异原因：同一时间创建，同一时间接收的订单
select count(*) from (
select bup.customer_id,
bup.last_order_sn,
tb.last_order_sn as order_sn,
bup.last_order_time,
tb.last_order_time as order_time,
bup.last_order_item_count,
tb.last_order_item_count as itemcount,
bup.last_order_store_id,
tb.last_order_store_id as store_id,
bup.last_order_storeno,
tb.last_order_storeno as store_no,
bup.last_order_eshop_id,
tb.last_order_eshop_id as eshop_id,
bup.last_order_city_code,
tb.last_order_city_code as city_code,
bup.last_order_channel_name,
tb.last_order_channel_name as channel_name,
bup.last_order_area_no,
tb.last_order_area_no as area_no from b_user_profile bup inner join  (
select m.customer_id,
    m.order_sn as last_order_sn,
    m.order_create_time as last_order_time,
    m.total_quantity as last_order_item_count,
    m.trading_price as last_order_trading_price,
    m.store_id as last_order_store_id,
    m.store_code as last_order_storeno,
    m.eshop_id as last_order_eshop_id,
    m.store_city_area_code as last_order_city_code,
    m.dcc_name as last_order_channel_name,
    m.area_no as last_order_area_no
    from (
select order_sn,customer_id,order_create_time,total_quantity,store_id,eshop_id,trading_price,sign_time,store_code,store_city_area_code,dcc_name,area_no, 
row_number() over (partition by customer_id order by order_create_time desc,sign_time desc) as rn
from v_full_order where sign_time is not null and sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00')
) m where m.rn = 1 
) tb on (bup.customer_id = tb.customer_id)
) tab where last_order_sn != order_sn;


--校验订单量  --
select * from (
select bup.customer_id,bup.order_count,t.count from b_user_profile bup inner join(
select count(distinct order_id) as count,customer_id from  v_full_order 
where sign_time is not null and sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') group by customer_id
) t on (t.customer_id = bup.customer_id) ) m where m.count != m.order_count;

--校验商品量 
select * from (
select bup.customer_id,bup.item_count,t.count from b_user_profile bup inner join(
select sum(total_quantity) as count,customer_id from 
(select *,row_number() over(partition by order_id order by order_create_time) as rn from  v_full_order) m
where sign_time is not null and sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') and rn = 1 group by customer_id
) t on (t.customer_id = bup.customer_id) ) m where m.item_count != m.count;

--校验金额 --有差异0.00000000000001
select * from (
select bup.customer_id,bup.trading_price_sum,t.count from b_user_profile bup inner join(
select sum(trading_price) as count,customer_id from 
(select *,row_number() over(partition by order_id order by order_create_time) as rn from  v_full_order) m
where sign_time is not null and sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') and rn = 1 group by customer_id
) t on (t.customer_id = bup.customer_id) ) m where m.trading_price_sum != m.count;

--校验消费过的门店数量
select * from (
select bup.customer_id,bup.store_count,t.count from b_user_profile bup inner join(
select count(distinct store_id) as count,customer_id from 
(select *,row_number() over(partition by order_id order by order_create_time) as rn from  v_full_order) m
where sign_time is not null and sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') and rn = 1 group by customer_id
) t on (t.customer_id = bup.customer_id) ) m where m.store_count != m.count;

--校验消费过的eshop数量
select * from (
select bup.customer_id,bup.eshop_count,t.count from b_user_profile bup inner join(
select count(distinct eshop_id) as count,customer_id from 
(select *,row_number() over(partition by order_id order by order_create_time) as rn from  v_full_order) m
where sign_time is not null and sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') and rn = 1 group by customer_id
) t on (t.customer_id = bup.customer_id) ) m where m.eshop_count != m.count;

--校验消费过的channel_count
select * from (
select bup.customer_id,bup.channel_count,t.count from b_user_profile bup inner join(
select count(distinct channel_id) as count,customer_id from 
(select *,row_number() over(partition by order_id order by order_create_time) as rn from  v_full_order) m
where channel_id != '' and sign_time is not null and sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') and rn = 1 group by customer_id
) t on (t.customer_id = bup.customer_id) ) m where m.channel_count != m.count;


--校验消费过的城市数量
select * from (
select bup.customer_id,bup.city_count,t.count from b_user_profile bup inner join(
select count(distinct store_city_area_code) as count,customer_id from 
(select *,row_number() over(partition by order_id order by order_create_time) as rn from  v_full_order) m
where sign_time is not null and sign_time <concat(from_unixtime(unix_timestamp(now()),"yyyy-MM-dd"), ' 00:00:00') and rn = 1 group by customer_id
) t on (t.customer_id = bup.customer_id) ) m where m.city_count != m.count;