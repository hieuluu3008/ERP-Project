-- DROP PROCEDURE y4a_erp.sp_prod_sales_invoice_02_2_wm_dsv_daily();
CREATE OR REPLACE PROCEDURE y4a_erp.sp_prod_sales_invoice_02_2_wm_dsv_daily()
LANGUAGE plpgsql
AS $procedure$
	begin
--Daily load cho snapshot log
insert into y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log
(
-- ITEM 360
with item_360 as (
		select
			supplier_stock_id
			, string_agg(distinct item_id, ',') as item_id
		from y4a_cdm.Y4A_DWB_WM_SUP_CAT_ITM
		where 1 = 1
		and fulfillment_method = 'DSV'
		and run_date = (select max(run_date) from y4a_cdm.Y4A_DWB_WM_SUP_CAT_ITM)
		and site_status <> 'UNPUBLISHED'
		group by 1)		
-- DIM_ORDER
,dim_order as (
		select * from (select purchase_order_id
							, count(*) as line_count
							, count(case when order_line_status in ('Delivered','Shipped') then purchase_order_id end) as success_line
							, count(case when order_line_status in ('Acknowledged') then purchase_order_id end) as acknowledged_line
							, count(case when order_line_status in ('Cancelled') then purchase_order_id end) as cancelled_line
					   from y4a_analyst.RP_WM_DSV_Order_DTL_BY_SKU where 1=1 and Order_date::date between '2023-06-01' and now()::date group by 1) z
		where success_line + cancelled_line = line_count and success_line != 0)
-- PROMO SSO SIN
,promo_sso_sin as (
		select * from y4a_analyst.wm_dsv_rollback_promotion_log
		where data_updated_time = ( select max(data_updated_time) from y4a_analyst.wm_dsv_rollback_promotion_log)
		)
-- PROMO DPL
, promo_dpl as (
		select
			'DPL' as team
			, "sku_supplier_stock_id" as sku
			, funding_amount::numeric
			, promo_start_1::date
			, promo_end_1::date
		from y4a_analyst.tb_gs_wm_promotion_dl
		where run_time = (select max(run_time) from  y4a_analyst.tb_gs_wm_promotion_dl)
		)
-- WM_DSV
,wm_dsv as (
		select
			purchase_Order_id
			, Order_line_id
			, Order_date
			, ship_datetime
			, CASE WHEN item_sku = '810073384521' THEN 'R61J' ELSE item_sku END item_sku
			, Order_line_quantity_amount
			, product_charge_amt
			, unit_cost_by_logic
			, ship_datetime::date ship_date
			, case when coalesce(y.item_id, z.item_id) is not null then coalesce(y.item_id, z.item_id) else 'Not found' end as item_id
		    , case when (CASE WHEN item_sku = '810073384521' THEN 'R61J' ELSE item_sku END) in (select distinct sku from promo_dpl) then a.unit_cost_by_logic + coalesce(c.funding_amount,0) else coalesce(b.current_cost, a.unit_cost_by_logic) end as unit_price
		from y4a_analyst.RP_WM_DSV_Order_DTL_BY_SKU
		left join item_360 y on (CASE WHEN a.item_sku = '810073384521' THEN 'R61J' ELSE a.item_sku END) = y.supplier_stock_id
		left join item_360 z on left((CASE WHEN a.item_sku = '810073384521' THEN 'R61J' ELSE a.item_sku END), 4) = z.supplier_stock_id
		left join promo_sso_sin b on position (b.item_id in coalesce(y.item_id, z.item_id)) > 0 and order_date between b.promo_start_1 and b.promo_end_1
		left join promo_dpl c on (CASE WHEN a.item_sku = '810073384521' THEN 'R61J' ELSE a.item_sku END) = c.sku and order_date::date between c.promo_start_1::date and c.promo_end_1::date
		where 1=1
		and Order_date::date between '2024-06-01' and now()::date
		and purchase_order_id  in (select distinct purchase_order_id from dim_order) -- chỉ chứa các order được được giao hoàn toàn
		and purchase_order_id not in (select distinct original_external_doc_no from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental where document_type = 'Order' and internal_sales_channel = 'WM DSV')
		order by purchase_Order_id
		)
select
	a.Order_date::date Order_date
	, null::date posting_date
	, a.Order_date::date document_date
	, 'Order' document_type
	, '131US-MER0002-WM' bill_to_customer
	, '131US-MER0002-WM' sell_to_customer
	, 'PLF-WM-1' platform
	, '1311-MER' customer_posting_group
	, null posting_description
	, '60D' payment_term
	, a.purchase_order_id external_doc_no
   , 'Unidentified' wh_location -- set location = 'Unidentified' sau đó thực hiện update sau khi ingest vào bảng temp
	, null currency
	, 'DATAMART_SI' source_code
	, 'CHAN-WMDSV' sales_channel
	, 'REG-USA' country
	, row_number() over(partition by a.purchase_Order_id)* 10000 line_no
	, 'ITEM' "type"
	, case when c.product_gtin is not null then left(c.supplier_stock_id,4) else left(a.item_sku, 4) end as "no" -- convert sang parent sku nếu là child sku
	, null description
	, 'pcs' uom
	, round(a.unit_cost_by_logic::numeric, 2) unit_price --ERP không dùng cột unit_price, do đó, dùng cột này để lưu lại net sales, phòng trường hợp sau này cần dùng tới
	, null::numeric line_discount_pct
	, null::numeric discount
	, a.Order_line_quantity_amount quantity
	, 'VAT-NOT' vat_product_posting_group
	, round((a.Order_line_quantity_amount * round(unit_price::numeric, 2))::numeric, 2) amount
	, null::numeric vat_amount
	, null batch_seq
	, a.purchase_Order_id po_number
	, true is_valid_record
	, now() data_updated_time
	, 'WM DSV' internal_sales_channel
	, a.purchase_Order_id original_external_doc_no
	, item_sku as asin
	, comp.company_id y4a_company_id
	, 'Y4ALLC' original_y4a_company_id
	, 'Y4A' belong_to_company
from wm_dsv a
left join y4a_erp.y4a_erp_dim_y4a_company_id comp on comp.company_code = 'Y4ALLC'
left join (select distinct product_gtin, supplier_stock_id
		   from y4a_cdm.Y4A_DWB_WM_SUP_CAT_ITM
		   where 1=1 and run_date = (select max(run_date) from y4a_cdm.Y4A_DWB_WM_SUP_CAT_ITM)) c on a.item_sku = c.product_gtin
where 1=1
)
;
--------------------------------------------------Xử lý giao dịch của Y4A, Hoang Thong-------------------------------------
--Daily load cho bảng temp
truncate y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp;
insert into y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp (
	Order_date
	, posting_date
	, document_date
	, document_type
	, bill_to_customer
	, sell_to_customer
	, platform
	, customer_posting_group
	, posting_description
	, payment_term
	, external_doc_no
	, wh_location
	, currency
	, source_code
	, sales_channel
	, country
	, line_no
	, "type"
	, "no"
	, description
	, uom
	, unit_price
	, line_discount_pct
	, discount
	, quantity
	, vat_product_posting_group
	, amount
	, vat_amount
	, batch_seq
	, po_number
	, is_valid_record
	, data_updated_time
	, internal_sales_channel
	, original_external_doc_no
	, asin
	, y4a_company_id
	, original_y4a_company_id
	, belong_to_company
	)
with excluded_invoice as (
	select distinct b.external_doc_no
	from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental b
	where UPPER(b.document_type) = 'ORDER'
union
	select distinct external_doc_no
	from y4a_erp.y4a_erp_prod_excluded_sales_invoice_header
)
select
	a.Order_date
--	, now()::date posting_date
	, CASE
       WHEN EXTRACT(MONTH FROM a.Order_date::date) < EXTRACT(MONTH FROM current_date::date) THEN date_trunc('month', now())::date
       WHEN EXTRACT(MONTH FROM a.Order_date::date) = EXTRACT(MONTH FROM current_date::date) THEN a.Order_date::date
       ELSE null
   END AS posting_date
	, a.document_date
	, a.document_type
	, a.bill_to_customer
	, a.sell_to_customer
	, a.platform
	, a.customer_posting_group
	, concat(split_part(a.country,'-',2) ,'-',split_part(a.sales_channel,'-',2),': Booking revenue, Inv#',a.external_doc_no,'_',to_char(now()::date,'YYMMDD')) posting_description
	, a.payment_term
	, a.external_doc_no
	, a.wh_location
	, a.currency
	, a.source_code
	, a.sales_channel
	, a.country
	, a.line_no
	, a."type"
	, a."no"
	, a.description
	, a.uom
	, a.unit_price
	, a.line_discount_pct
	, a.discount
	, a.quantity
	, a.vat_product_posting_group
	, a.amount
	, a.vat_amount
	, a.batch_seq
	, a.po_number
	, a.is_valid_record
	, a.data_updated_time
	, a.internal_sales_channel
	, a.original_external_doc_no
	, a.asin
	, a.y4a_company_id
	, a.original_y4a_company_id
	, a.belong_to_company
from y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log a
left join excluded_invoice b on a.external_doc_no = b.external_doc_no
where a.data_updated_time = (select max(data_updated_time)
							 from y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log)
	and a.is_valid_record = true
	and b.external_doc_no is null
	and a.belong_to_company in ('Y4A', 'Hoang Thong', 'JTO')
	and a.Order_date::date between '2023-06-01' and now()::date;
-- Update Location
update y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp a
set wh_location = b.location
from (
	with txn as(
	select
		distinct transaction_id
		, case
			when warehouse in('ANA', 'BE-ANA', 'BRE') then '156-USA-CA'
			when warehouse = 'CRA' then '156-3PL-CB'
			when warehouse = 'FLO' then '156-3PL-FR'
			when warehouse = 'FON' then '156-3PL-FT'
			when warehouse = 'CQM' then '156-3PL-CA'
			else 'Unidentified'
		end location
	from y4a_sop_ops.transaction_yes4all_detail
	where 1=1
		and	"transaction" = 'WM DSV'
		and log_date >= '2024-01-01'
		and ims_id is not null
		and qty <> 0
		and case when ims_id = 0 and log_id = 0 then false else true end
		)
	select
		transaction_id
		, count(distinct location) cnt
		, string_agg(distinct location,'') location
	from txn
	group by 1
	having count(distinct location) = 1
	) b
where a.po_number = b.transaction_id and a.wh_location = 'Unidentified'
;
	
--Daily load cho sales invoice header
insert into y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily (
	Order_date
	, posting_date
	, document_date
	, document_type
	, bill_to_customer
	, sell_to_customer
	, platform
	, customer_posting_group
	, posting_description
	, payment_term_code
	, external_doc_no
	, "location"
	, currency
	, source_code
	, sales_channel
	, country
	, is_valid_record
	, data_updated_time
	, internal_sales_channel
	, original_external_doc_no
	, y4a_company_id
	, original_y4a_company_id
	, belong_to_company
	, is_processed
)
select distinct
	Order_date
	, posting_date
	, document_date
	, document_type
	, bill_to_customer
	, sell_to_customer
	, platform
	, customer_posting_group
	, posting_description
	, payment_term
	, external_doc_no
	, wh_location  as "location"
	, currency
	, source_code
	, sales_channel
	, country
	, is_valid_record
	, data_updated_time
	, internal_sales_channel
	, original_external_doc_no
	, y4a_company_id
	, original_y4a_company_id
	, belong_to_company
	, 0 as is_processed
from y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp a
;
--Daily load cho sales invoice line
insert into y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily (
	document_no
	, line_no
	, "type"
	, "no"
	, description
	, uom
	, unit_price
	, line_discount_pct
	, discount
	, quantity
	, location_code
	, vat_product_posting_group
	, amount
	, vat_amount
	, country
	, sales_channel
	, platform
	, batch_seq
	, po_number
	, data_updated_time
	, original_asin_sku
)
	select distinct
	external_doc_no as document_no
	, line_no
--Với Extra Invoice (DI Invoice nhưng không có mua hàng), Doanh thu sẽ được tính
--Trong doanh thu khác, không phải gross sales
	, "type"
	, "no"
	, description
	, uom
	, round(unit_price::numeric,2) unit_price
	, line_discount_pct
	, discount
	, quantity
	, wh_location location_code
	, vat_product_posting_group
	, round(amount::numeric,2) amount
	, vat_amount
	, country
	, sales_channel
	, platform
	, batch_seq
	, po_number
	, data_updated_time
	, asin original_asin_sku
from y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp  a
;
-----------------------------------------Xử lý giao dịch On-behalf - HMD & Moonkey--------------------------------
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
	now()::date posting_date ,
	a.document_date ,
	a.document_type ,
	a.bill_to_customer ,
	a.sell_to_customer ,
	a.platform ,
	a.customer_posting_group,
	concat(split_part(a.country,'-',2) ,'-',split_part(a.sales_channel,'-',2),': Booking revenue, Inv#',a.external_doc_no,'_',to_char(now()::date,'YYMMDD')) posting_description ,
	a.payment_term ,
	a.external_doc_no,
	null wh_location,
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
		and (a.belong_to_company not in ('Y4A', 'Hoang Thong', 'JTO') and a.belong_to_company is not null)
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
update y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily set uom = null where "no" in ('33890101','71190101','52120101');
--update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
--set posting_date = (select max(to_char(posting_date,'YYYY-MM-DD'))::date from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily)
--where to_char(posting_date,'YYYY-MM') != (select max(to_char(posting_date,'YYYY-MM')) from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily);
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set is_processed = 0 where is_processed is null ;
---2024 Changes
---update customer_posting_group OBH
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set customer_posting_group = '1311-MER';
---update name OBH
--update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
--set name = '331US-MER0019-NGU' where belong_to_company = 'General Union';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = 'US-MER0002-BLS' where belong_to_company = 'Bluestars';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = 'US-SER0002-BTT' where belong_to_company = 'Bitis';
--
--update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
--set name = 'US-SER0006-VTX' where belong_to_company = 'Vitox';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = '331US-SER0019-A&D' where belong_to_company = 'A&D';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = 'US-SER0003-HUR' where belong_to_company = 'HMD';
	END;
$procedure$
;

