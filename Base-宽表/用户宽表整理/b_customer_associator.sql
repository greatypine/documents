insert overwrite b_customer_associator partition (create_date)
select
	cc.customer_id,
	cc.city_code,
	cc.city_no,
	cc.opencard_store_id,
	cc.idcard,
	cc.birthday,
	cc.create_time,
	cc.create_date
from (
	select
		tmp.customer_id,
		sc.city_code,
		tmp.city_no,
		tmp.opencard_store_id,
		tmp.create_time,
		tmp.create_date,
		tmp.idcard,
		tmp.birthday,
		row_number() over(partition by tmp.customer_id order by tmp.create_time asc) as amount_num
	from (
		select
			tab.id as customer_id,
			ifnull(ifnull(tab.torp_city_code,tab.tcard_city_code),tab.city_code) as city_no,
			ifnull(tab.torp_store_id,tcard_store_id) as opencard_store_id,
			cast(ifnull(ifnull(tab.torp_pay_time,tab.createdat),tab.begin_time) as string) as create_time,
			to_date(ifnull(ifnull(tab.torp_pay_time,tab.createdat),tab.begin_time)) as create_date,
			tab.idcard,
			tab.birthday
		from (
			select
				tb.id,
				tc.city_code,
				tc.createdat,
				tc.idcard,
				tc.birthday,
				tm.begin_time,
				tb.associator_expiry_date,
				tp.store_id as torp_store_id,
				tp.type as torp_type,
				tp.pay_time as torp_pay_time,
				tp.city_code as torp_city_code,
				tcd.store_id as tcard_store_id,
				tcd.city_code as tcard_city_code,
				tcd.type as tcard_type
			from (
				select
					vc.id,
					vc.associator_expiry_date
				from v_full_customer as vc
				where vc.associator_level = 2
			) as tb
			left join (
				SELECT t.customer_id,
			           t.store_id,
			           t.type,
			           t.pay_time,
			           t.city_code
			   	FROM
			     ( SELECT customer_id,
			              torp.store_id,
			              torp.type,
			              torp.pay_time,
			              ts.city_code,
			              ROW_NUMBER() OVER (partition BY torp.customer_id
			                                 ORDER BY torp.create_time ASC) AS rn
			      FROM gemini.t_order_receipts torp
			      LEFT JOIN gemini.t_store ts ON (ts.id = torp.store_id)
			      WHERE torp.pay_status ='payed'
			        AND (torp.type = 'associator_start_2'
			             OR torp.type = 'associator_up_2'
			             OR torp.type='plus') ) t
			   WHERE t.rn = 1 
			) tp on tp.customer_id = tb.id
			left join (
				select 
					t.customer_id,
					t.type,
					t.store_id,
					t.city_code 
				from ( select 
						tcard.customer_id,
						tcard.type,
						tbatch.store_id,
						ts.city_code,
						ROW_NUMBER() OVER (partition BY customer_id ORDER BY tcard.create_time ASC) as rn
					from gemini.t_card tcard
					join gemini.t_exchange_card_batch tbatch on (tcard.batch_id = tbatch.id)
					left join gemini.t_store ts ON (ts.id = tbatch.store_id) where tcard.customer_id is not null 
				) t where t.rn = 1
			) tcd on tcd.customer_id = tb.id
			left join (
				select customerid,createdat,citycode as city_code,idcard,birthday from (
					select customerid,createdat,citycode,idcard,birthday,ROW_NUMBER() OVER (partition BY customerid ORDER BY createdat desc) as rn
					from gemini_mongo.t_customer_info_record_ext
				) t where rn = 1
			) as tc on tb.id = tc.customerid
			left join (
				select customer_id, max(create_time) as begin_time from gemini.t_member_operation_record where level = 2 and status = 0 group by customer_id
			) as tm on tb.id = tm.customer_id
		) as tab
	) as tmp
	left join t_sys_citycode as sc on sc.city_tel_code = tmp.city_no
) as cc 
where cc.amount_num = 1 
;



drop table if exists b_customer_associator;
create table if not exists b_customer_associator ( 
customer_id string,
city_code string,
city_no string,
opencard_store_id string,
idcard string,
birthday string,
create_time string ) partitioned by(create_date string);