-- DROP PROCEDURE y4a_erp.sp_prod_sales_invoice_02_2_wayfair_daily();
CREATE OR REPLACE PROCEDURE y4a_erp.sp_prod_sales_invoice_02_2_wayfair_daily()
LANGUAGE plpgsql
AS $procedure$
	begin
---------------------------------------------------Sales Invoice Line-----------------------------------------------------	
--Daily load cho snapshot log
insert
	into
	y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log
--Wayfair
(
with wf_gross_sale as(
select * from y4a_erp.y4a_erp_view_si_wayfair_base_cost
)
select
	a.invoice_date::date Order_date,
	null::date posting_date,
	a.invoice_date::date document_date,
	'Order' document_type,
	'131US-MER0001-WF' bill_to_customer,
	'131US-MER0001-WF' sell_to_customer,
	'PLF-WF-1' platform,
	e.customer_posting_group,
	null posting_description,
	'60D' as payment_term,
	case
		when e.belong_to_company in ('Y4A', 'Hoang Thong','General Union', 'JTO') then a.invoice_num
		when (e.belong_to_company not in ('Y4A', 'Hoang Thong','General Union','JTO')
			and e.belong_to_company is not null) then concat(a.invoice_num, '-OBH-', e.belong_to_company)
	end external_doc_no,
	case
		when e.belong_to_company = 'Bluestars' then '156-OBH-CA'
		 when b.po_number not in (select distinct order_id from (select order_id, warehouse,row_number() over(partition by order_id order by run_date) as rn from y4a_cdm.y4a_dwb_wyf_dso ) z where rn = 1) and e.belong_to_company not in ('Y4A', 'Hoang Thong','General Union','JTO') then '157-OBH' -- WWF CG
		 when b.po_number not in (
			select distinct order_id from (select order_id, warehouse, row_number() over(partition by order_id order by run_date) as rn from y4a_cdm.y4a_dwb_wyf_dso ) z where rn = 1)
			 and e.belong_to_company in ('Y4A', 'Hoang Thong','General Union','JTO') then
				case when a.account_name = 'PRSEnterprisesLLC' then '157-WFC-US'
					 when a.account_name = 'CAN_CAN_PRSEnterprisesLLC' then '157-WF-CAN'
				end -- WF CG
		 else 'Unidentified'
	     end as wh_location,
	case
		when a.currency = 'USD' then null
		else a.currency
	end currency,
	'DATAMART_SI' source_code,
	'CHAN-WAYFAIR' sales_channel,
	case
		when a.account_name = 'PRSEnterprisesLLC' then 'REG-USA'
		when a.account_name = 'CAN_CAN_PRSEnterprisesLLC' then 'REG-CAN'
	end country,
	row_number() over(partition by case
		when e.belong_to_company in ('Y4A', 'Hoang Thong','General Union','JTO') then a.invoice_num
		else concat(a.invoice_num, '-OBH-', e.belong_to_company)
	end)* 10000 line_no,
	'ITEM' "type",
	left(b.item_number, 4) "no",
-- convert sang parent sku nếu là child sku
	'' description,
	'pcs' uom,
--ERP không dùng cột unit_price, do đó, dùng cột này để lưu lại net sales, phòng trường hợp sau này cần dùng tới
b.wholesale_price unit_price,
	null::numeric line_discount_pct,
	null::numeric discount,
	b.quantity,
	'VAT-NOT' vat_product_posting_group,
	round((c.base_cost * b.quantity)::numeric, 2) amount,
	null::numeric vat_amount,
	null batch_seq,
	a.po_num po_number,
	true is_valid_record ,
	now() data_updated_time,
	case when b.po_number not in (select distinct order_id from (select order_id,row_number() over(partition by order_id order by run_date) as rn from y4a_cdm.y4a_dwb_wyf_dso ) z where rn = 1)
		then 'WF CG' else 'WF' end as internal_sales_channel,
	a.invoice_num original_external_doc_no,
	item_number as asin,
	comp.company_id y4a_company_id,
	'PR-ENTERPRISE' original_y4a_company_id,
	e.belong_to_company
from
	y4a_cdm.y4a_dwb_wyf_fin_inv_dsv_upt a
left join y4a_cdm.y4a_dwb_wyf_ord_exp_upt b on
	a.po_num = b.po_number
left join wf_gross_sale c on
b.item_number::text = c.supplier_part_number::text
and b.po_date_time between effective_from and effective_to
and a.account_name = c.account_name
left join y4a_erp.y4a_erp_dim_y4a_company_id comp on
comp.company_code = 'PR-ENTERPRISE'
left join ( SELECT DISTINCT y4a_erp_prod_master_data_item.item_id,
           y4a_erp_prod_master_data_item.original_y4a_company_id,
           y4a_erp_prod_master_data_item.belong_to_company, customer_posting_group
          FROM y4a_erp.y4a_erp_prod_master_data_item) e on
	left(b.item_number,4) = e.item_id
	and e.original_y4a_company_id = 'Y4ALLC'
where 1=1
and a.amount > 0
AND b.order_status != 'Cancelled'
and a.invoice_date::date between '2024-06-01' and now()::date
and right(a.invoice_num,3) not like '%CM%'
and a.invoice_num not like '%SAP Credit%'
--and b.order_status <> 'Cancelled' --remove cac sku bi cancel trong order
and a.invoice_num not in ('CS463102421_43696161') -- exceptional cases -- from ACCT x D&D Aligment
)
;
--------------------------------------------------Xử lý giao dịch của Y4A, Hoang Thong, GU-------------------------------------
--Daily load cho bảng temp
truncate y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp;
insert
	into
	y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp
	(Order_date ,
	posting_date ,
	document_date ,
	document_type ,
	bill_to_customer ,
	sell_to_customer ,
	platform ,
	customer_posting_group,
	posting_description ,
	payment_term ,
	external_doc_no,
	wh_location,
	currency ,
	source_code ,
	sales_channel,
	country ,
	line_no ,
	"type" ,
	"no",
	description,
	uom,
	unit_price,
	line_discount_pct,
	discount,
	quantity,
	vat_product_posting_group,
	amount,
	vat_amount ,
	batch_seq ,
	po_number ,
	is_valid_record ,
	data_updated_time,
	internal_sales_channel,
	original_external_doc_no ,
	asin,
	y4a_company_id,
	original_y4a_company_id,
	belong_to_company)
with excluded_invoice as(
	select
		distinct b.external_doc_no
	from
		y4a_erp.y4a_erp_prod_sales_invoice_header_incremental b
	where
		UPPER(b.document_type) = 'ORDER' and internal_sales_channel in ('WF', 'WF CG')
union
	select
		distinct external_doc_no
	from
		y4a_erp.y4a_erp_prod_excluded_sales_invoice_header
)
select
	a.Order_date ,
--	a.Order_date::date as posting_date ,
	CASE
       WHEN EXTRACT(MONTH FROM a.Order_date::date) < EXTRACT(MONTH FROM current_date::date) THEN date_trunc('month', now())::date
       WHEN EXTRACT(MONTH FROM a.Order_date::date) = EXTRACT(MONTH FROM current_date::date) THEN a.Order_date::date
       ELSE null
   END AS posting_date,
	a.document_date ,
	a.document_type ,
	a.bill_to_customer ,
	a.sell_to_customer ,
	a.platform ,
	a.customer_posting_group,
	concat(split_part(a.country,'-',2) ,'-',split_part(a.sales_channel,'-',2),': Booking revenue, Inv#',a.external_doc_no,'_',to_char(a.Order_date,'YYMMDD')) posting_description ,
	a.payment_term ,
	a.external_doc_no,
	a.wh_location,
	a.currency ,
	a.source_code ,
	a.sales_channel,
	a.country ,
	a.line_no ,
	a."type" ,
	a."no",
	a.description,
	a.uom,
	a.unit_price,
	a.line_discount_pct,
	a.discount,
	a.quantity,
	a.vat_product_posting_group,
	a.amount,
	a.vat_amount ,
	a.batch_seq ,
	a.po_number ,
	a.is_valid_record ,
	a.data_updated_time,
	a.internal_sales_channel,
	a.original_external_doc_no ,
	a.asin,
	a.y4a_company_id,
	a.original_y4a_company_id,
	a.belong_to_company
from
	y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log a
	left join excluded_invoice b on a.external_doc_no = b.external_doc_no
where
	a.data_updated_time = (
	select
		max(data_updated_time)
	from
		y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log)
		and a.is_valid_record = true
		and b.external_doc_no is null
		and a.belong_to_company in ('Y4A', 'Hoang Thong','General Union','JTO')
		and a.Order_date::date between '2024-06-01'  and now()::date
		;
-- UPDATE LOCATION
update y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp a
set wh_location = b.location
from (with txn as(
select
	distinct transaction_id ,
	case
		when warehouse in('ANA', 'BE-ANA', 'BRE') then '156-USA-CA'
		when warehouse = 'CRA' then '156-3PL-CB'
		when warehouse = 'FLO' then '156-3PL-FR'
		when warehouse = 'FON' then '156-3PL-FT'
		when warehouse = 'CQM' then '156-3PL-CA'
		else 'Unidentified'
	end location
from
		y4a_sop_ops.transaction_yes4all_detail
where
		"transaction" = 'WF DS'
	and log_date >= '2024-01-01'
	and ims_id is not null
	and qty <> 0
	and case when ims_id = 0 and log_id = 0 then false else true end
)select
	transaction_id ,
	count(distinct location) cnt,
	string_agg(distinct location,'') location
from
	txn
group by
	1
having count(distinct location) = 1) b
where a.original_external_doc_no = b.transaction_id and a.wh_location = 'Unidentified'
;
	
	
	
--Daily load cho sales invoice header
insert
	into
	y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily (
	Order_date,
	posting_date ,
	document_date,
	document_type,
	bill_to_customer ,
	sell_to_customer,
	platform,
	customer_posting_group,
	posting_description,
	payment_term_code,
	external_doc_no ,
	"location" ,
	currency ,
	source_code ,
	sales_channel ,
	country ,
	is_valid_record ,
	data_updated_time,
	internal_sales_channel,
	original_external_doc_no,
	y4a_company_id,
	original_y4a_company_id,
	belong_to_company,
	is_processed
)
select
	distinct
	Order_date,
	posting_date ,
	document_date,
	document_type,
	bill_to_customer ,
	sell_to_customer,
	platform,
	customer_posting_group,
	posting_description,
	payment_term,
	external_doc_no ,
	wh_location as "location" ,
	currency ,
	source_code ,
	sales_channel ,
	country ,
	is_valid_record ,
	data_updated_time,
	internal_sales_channel,
	original_external_doc_no,
	y4a_company_id,
	original_y4a_company_id,
	belong_to_company,
	0 as is_processed
from
	y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp a
	;
--Daily load cho sales invoice line
insert
	into
	y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily (
	document_no,
	line_no ,
	"type",
	"no" ,
	description ,
	uom ,
	unit_price ,
	line_discount_pct ,
	discount ,
	quantity,
	location_code,
	vat_product_posting_group ,
	amount ,
	vat_amount ,
	country ,
	sales_channel ,
	platform,
	batch_seq ,
	po_number,
	data_updated_time,
	original_asin_sku
)
	select
	distinct external_doc_no as document_no,
	line_no ,
	"type",
	"no" ,
	description ,
	uom ,
	round(unit_price::numeric,2) unit_price ,
	line_discount_pct ,
	discount ,
	quantity,
	wh_location location_code,
	vat_product_posting_group ,
	round(amount::numeric,2) amount,
	vat_amount ,
	country ,
	sales_channel ,
	platform,
	batch_seq ,
	po_number,
	data_updated_time,
	asin original_asin_sku
from
	y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp  a ;
-----------------------------------------Xử lý giao dịch On-behalf - Bluestars, Bitis--------------------------------
--Daily insert bảng on behalf
delete from y4a_erp.y4a_erp_prod_obh_sales_invoice_incremental;
insert into
	y4a_erp.y4a_erp_prod_obh_sales_invoice_incremental
	(Order_date ,
	posting_date ,
	document_date ,
	document_type ,
	bill_to_customer ,
	sell_to_customer ,
	platform ,
	customer_posting_group,
	posting_description ,
	payment_term ,
	external_doc_no,
	wh_location,
	currency ,
	source_code ,
	sales_channel,
	country ,
	line_no ,
	"type" ,
	"no",
	description,
	uom,
	unit_price,
	line_discount_pct,
	discount,
	quantity,
	vat_product_posting_group,
	amount,
	vat_amount ,
	batch_seq ,
	po_number ,
	is_valid_record ,
	data_updated_time,
	internal_sales_channel,
	original_external_doc_no ,
	asin,
	y4a_company_id,
	original_y4a_company_id,
	belong_to_company)
select
	a.Order_date ,
	CASE
       WHEN EXTRACT(MONTH FROM a.Order_date::date) < EXTRACT(MONTH FROM current_date::date) THEN date_trunc('month', now())::date
       WHEN EXTRACT(MONTH FROM a.Order_date::date) = EXTRACT(MONTH FROM current_date::date) THEN a.Order_date::date
       ELSE null
   END AS posting_date,
	a.document_date ,
	a.document_type ,
	a.bill_to_customer ,
	a.sell_to_customer ,
	a.platform ,
	a.customer_posting_group,
	concat(split_part(a.country,'-',2) ,'-',split_part(a.sales_channel,'-',2),': Booking revenue, Inv#',a.external_doc_no,'_',to_char(a.Order_date,'YYMMDD')) posting_description ,
	a.payment_term ,
	a.external_doc_no,
	case when a.belong_to_company = 'HMD' then null else a.wh_location end as wh_location ,
	a.currency ,
	a.source_code ,
	a.sales_channel,
	a.country ,
	a.line_no ,
	a."type" ,
	a."no",
	a.description,
	a.uom,
	a.unit_price,
	a.line_discount_pct,
	a.discount,
	a.quantity,
	a.vat_product_posting_group,
	a.amount,
	a.vat_amount ,
	a.batch_seq ,
	a.po_number ,
	a.is_valid_record ,
	a.data_updated_time,
	a.internal_sales_channel,
	a.original_external_doc_no ,
	a.asin,
	a.y4a_company_id,
	a.original_y4a_company_id,
	a.belong_to_company
from
	y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log a
	left join y4a_erp.y4a_erp_prod_sales_invoice_header_incremental b on a.external_doc_no = b.external_doc_no
where
	a.data_updated_time = (
	select
		max(data_updated_time)
	from
		y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log)
		and a.is_valid_record = true
		and b.external_doc_no is null
		and (a.belong_to_company not in ('Y4A', 'Hoang Thong','General Union','JTO') and a.belong_to_company is not null)
		and a.Order_date::date between '2023-06-01' and now()::date
		;
--Insert giao dịch On-behalf vào header
insert into y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily (
	Order_date,
	posting_date ,
	document_date,
	document_type,
	bill_to_customer ,
	sell_to_customer,
	platform,
	customer_posting_group,
	posting_description,
	payment_term_code,
	external_doc_no ,
	"location" ,
	currency ,
	source_code ,
	sales_channel ,
	country ,
	is_valid_record ,
	data_updated_time,
	internal_sales_channel,
	original_external_doc_no,
	y4a_company_id,
	original_y4a_company_id,
	belong_to_company,
	"name"
)select distinct
	Order_date,
	posting_date ,
	document_date,
	document_type,
	bill_to_customer ,
	sell_to_customer,
	platform,
	customer_posting_group,
	posting_description,
	payment_term,
	external_doc_no ,
	wh_location ,
	currency ,
	source_code ,
	sales_channel ,
	country ,
	is_valid_record ,
	data_updated_time,
	internal_sales_channel,
	original_external_doc_no,
	y4a_company_id,
	original_y4a_company_id,
	belong_to_company,
	bill_to_customer "name"
	from y4a_erp.y4a_erp_prod_obh_sales_invoice_incremental;
--Daily load cho sales invoice line OBH
insert
	into
	y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily (
	document_no,
	line_no ,
	"type",
	"no" ,
	description ,
	uom ,
	unit_price ,
	line_discount_pct ,
	discount ,
	quantity,
	location_code,
	vat_product_posting_group ,
	amount ,
	vat_amount ,
	country ,
	sales_channel ,
	platform,
	batch_seq ,
	po_number,
	data_updated_time,
	original_asin_sku
)select
	distinct external_doc_no as document_no,
	line_no ,
	'G/L Account' "type",
	'33890101' "no" ,
	description ,
	null uom ,
	round(unit_price::numeric,2) unit_price ,
	line_discount_pct ,
	discount ,
	1 quantity,
	wh_location location_code,
	vat_product_posting_group ,
	round(amount::numeric,2) amount,
	vat_amount ,
	country ,
	sales_channel ,
	platform,
	batch_seq ,
	po_number,
	data_updated_time,
	asin original_asin_sku
from
	y4a_erp.y4a_erp_prod_obh_sales_invoice_incremental;
--- Line trừ tồn OBH
insert
	into
	y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily (
	document_no,
	line_no ,
	"type",
	"no" ,
	description ,
	uom ,
	unit_price ,
	line_discount_pct ,
	discount ,
	quantity,
	location_code,
	vat_product_posting_group ,
	amount ,
	vat_amount ,
	country ,
	sales_channel ,
	platform,
	batch_seq ,
	po_number,
	data_updated_time,
	original_asin_sku
)select
	distinct external_doc_no as document_no,
	line_no ,
	'ITEM' "type",
	"no" ,
	description ,
	'pcs' uom ,
	round(unit_price::numeric,2) unit_price ,
	line_discount_pct ,
	discount ,
	quantity,
	wh_location location_code,
	vat_product_posting_group ,
	0 amount,
	vat_amount ,
	country ,
	sales_channel ,
	platform,
	batch_seq ,
	po_number,
	data_updated_time,
	asin original_asin_sku
from
	y4a_erp.y4a_erp_prod_obh_sales_invoice_incremental
where belong_to_company in ('Bluestars', 'Bitis');
update y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily set uom = null where "no" in ('33890101','71190101','52120101');
--update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
--set posting_date = (select max(to_char(posting_date,'YYYY-MM-DD'))::date from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily)
--where to_char(posting_date,'YYYY-MM') != (select max(to_char(posting_date,'YYYY-MM')) from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily);
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set is_processed = 0 where is_processed is null ;
--2024 changes
--update customer_posting_group OBH
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set customer_posting_group = '1311-MER';
---update name OBH
--update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
--set name = '331US-MER0019-NGU' where belong_to_company = 'General Union';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = 'US-MER0002-BLS' where belong_to_company = 'Bluestars';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = 'US-SER0002-BTT' where belong_to_company = 'Bitis';
--update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
--set name = 'US-SER0006-VTX' where belong_to_company = 'Vitox';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = '331US-SER0019-A&D' where belong_to_company = 'A&D';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = 'US-SER0003-HUR' where belong_to_company = 'HMD';
	END;
$procedure$
;

