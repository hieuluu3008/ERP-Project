-- DROP PROCEDURE y4a_erp.sp_prod_sales_invoice_02_2_avc_di_wh_daily();
CREATE OR REPLACE PROCEDURE y4a_erp.sp_prod_sales_invoice_02_2_avc_di_wh_daily()
LANGUAGE plpgsql
AS $procedure$
	begin
--Daily load cho snapshot log
insert
	into
	y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log
	--AVC DI AND WH		
(
with vendor as(
--Mục đích để xác định kênh DI hay kênh WH
	select
		po_id po,
		case
			when vendor in ('YES4A', 'YES4M') then 'AVC WH'
			when vendor in ('YEX5L') then 'AVC GPE'
			when vendor is null then 'Unidentified'
			else 'AVC DI'
		end sales_channel
	from
		y4a_finance.y4a_dwc_amz_avc_purchase_order
	group by
		1,
		2),
		-- Lọc những invoice lệch giữa inv amount sum và inv amount dtl,
	avc_tbl as(
	select
		a.invoice_date,
		case
			when c.sales_channel IN ('AVC DI', 'AVC GPE') then '120D'
			else '90D'
		end as payment_term,
		a.invoice_number,
		a.country,
		coalesce(pims.pims_sku,eu_case.sku) as pims_sku,
		b.invoice_price/b.invoice_quantity unit_cost,
		b.invoice_quantity quantity ,
		b.invoice_price total_amount,
		b.purchase_order_id po_number,
		c.sales_channel,
		b.asin,
		comp.company_id y4a_company_id,
		'Y4ALLC' original_y4a_company_id,
		item_master.belong_to_company,
		item_master.customer_posting_group
	from
		y4a_finance.y4a_dwc_amz_avc_invoice a
	left join
		y4a_finance.y4a_dwc_amz_avc_invoice_detail b on
		a.invoice_number = b.invoice_number
	left join vendor c on
		b.purchase_order_id = c.po
	left join y4a_finance.dim_pims_platform_product_profile_by_country pims on
		b.asin = pims.platform_prd_id
		and a.country = pims.country
		and pims.country = 'USA'
	left join (select distinct amz_po , asin, sku from y4a_analyst.tb_poms_com_inv_dri) eu_case on
		eu_case.amz_po = b.purchase_order_id
		and eu_case.asin = b.asin
	left join y4a_erp.y4a_erp_dim_y4a_company_id comp on
		comp.company_code = 'Y4ALLC'
--item_master dùng để lấy các dimension quan trọng của SKU, bao gồm:
--customer_posting_group
	left join ( SELECT DISTINCT y4a_erp_prod_master_data_item.item_id,
           y4a_erp_prod_master_data_item.original_y4a_company_id,
           y4a_erp_prod_master_data_item.belong_to_company,
            customer_posting_group
          FROM y4a_erp.y4a_erp_prod_master_data_item) item_master on
		coalesce(pims.pims_sku,eu_case.sku) = item_master.item_id
		and item_master.original_y4a_company_id = 'Y4ALLC'
--	left join inv_check on
--		a.invoice_number = inv_check.invoice_number
	WHERE 1=1
--		inv_check.invoice_number is null
		and b.invoice_number not like '%SCR'
		and b.invoice_number not like '%PCR'
		and a.invoice_number not in ('72NSHPXZ_LTN1','7SV832EE_LTN','7P6OUTOA_LTN','72NSHPXZ_LTR','RL67_BOL','25NVILUZ_RL9','38EDQ4SF_RF97','36ES7JTV_TO53')
--		and a.invoice_status not like '%Paid%'
		and a.invoice_date::date between '2025-01-01' and now()::date
--		and a.is_deleted is null
		)
	select
		a.invoice_date::date Order_date,
		null::date posting_date  ,
		a.invoice_date::date document_date,
		'Order' document_type,
		'131US-MER0001-AMZ' bill_to_customer,
		'131US-MER0001-AMZ' sell_to_customer,
		'PLF-AMZ-1' platform,
		customer_posting_group,
		null posting_description,
		payment_term,
		case
			when a.belong_to_company in ('Y4A', 'Hoang Thong','JTO') then a.invoice_number
			else concat(a.invoice_number, '-OBH-', a.belong_to_company)
		end external_doc_no,
		case
			when a.belong_to_company in ('Y4A', 'Hoang Thong','JTO') then
case
				when a.sales_channel = 'AVC DI' then
case
					when a.country = 'USA' then '157-DI-USA'
					when a.country = 'SGP' then '157-DI-SGP'
					when a.country = 'ARE' then '157-DI-UAE'
					when a.country = 'AUS' then '157-DI-AUS'
					when a.country = 'CAN' then '157-DI-CAN'
					when a.country = 'DEU' then '157-DI-DEU'
					when a.country = 'GBR' then '157-DI-GBR'
					when a.country = 'JPN' then '157-DI-JPN'
					when a.country = 'MEX' then '157-DI-MXN'
					when a.country = 'ITA' then '157-DI-ITA'
					when a.country = 'ESP' then '157-DI-ESP'
					when a.country = 'FRA' then '157-DI-FRA'
					when a.country = 'SAU' then '157-DI-SAU'
				end
				when a.sales_channel = 'AVC GPE' then '157-DI-GPE'
				when a.sales_channel = 'AVC WH'
			then 'Unidentified'
			end
			when UPPER(a.belong_to_company) in ('HMD', 'MOONKEY') then null
			WHEN a.belong_to_company IS NULL THEN NULL
			else '156-OBH-CA'
		end as wh_location,
		null currency,
		'DATAMART_SI' source_code,
		'CHAN-AVC' sales_channel,
		case
			when a.sales_channel in ('AVC DI', 'AVC GPE')  then
																case
																	when a.country = 'USA' then 'REG-USA'
																	when a.country = 'SGP' then 'REG-SGP'
																	when a.country = 'ARE' then 'REG-UAE'
																	when a.country = 'AUS' then 'REG-AUS'
																	when a.country = 'CAN' then 'REG-CAN'
																	when a.country = 'DEU' then 'REG-DEU'
																	when a.country = 'GBR' then 'REG-GBR'
																	when a.country = 'JPN' then 'REG-JPN'
																	when a.country = 'MEX' then 'REG-MXN'
																	when a.country = 'ITA' then 'REG-ITA'
																	when a.country = 'ESP' then 'REG-ESP'
																	when a.country = 'FRA' then 'REG-FRA'
																	when a.country = 'SAU' then 'REG-SAU'
																	else 'Unidentified'
																end
			when a.sales_channel = 'AVC WH' then 'REG-USA'
				else 'Unidentified'
		end country,
		row_number() over(partition by case
			when a.belong_to_company in ('Y4A', 'Hoang Thong','JTO') then a.invoice_number
			else concat(a.invoice_number, '-OBH-', a.belong_to_company)
		end)* 10000 line_no,
		'ITEM' "type",
		pims_sku as "no",
		null description,
		'pcs' uom,
		round(unit_cost::numeric, 2) unit_price,
		null::numeric line_discount_pct,
		null::numeric discount,
		a.quantity,
		'VAT-NOT' vat_product_posting_group,
		round(total_amount::numeric, 2) amount,
		null::numeric vat_amount,
		null batch_seq,
		po_number,
		true is_valid_record ,
		-- loại bỏ toàn bộ invoice line nếu không thỏa điều kiện
		now() data_updated_time,
		sales_channel internal_sales_channel,
		a.invoice_number original_external_doc_no,
		asin,
		a.y4a_company_id,
		a.original_y4a_company_id,
		a.belong_to_company
	from
		avc_tbl a
)
;
--------------------------------------------------Xử lý giao dịch của Y4A, Hoang Thong-------------------------------------
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
		1=1
		and (UPPER(b.document_type) = 'ORDER' OR UPPER(b.document_type) = 'ACCRUAL')
union
	select
		distinct external_doc_no
	from
		y4a_erp.y4a_erp_prod_excluded_sales_invoice_header
)
select
	a.Order_date ,
--	now()::date posting_date ,
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
	concat(split_part(a.country,'-',2) ,'-',split_part(a.sales_channel,'-',2),': Booking revenue, Inv#',a.external_doc_no,'_',to_char(now()::date,'YYMMDD')) posting_description ,
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
		and a.belong_to_company in ('Y4A', 'Hoang Thong','JTO')
		and a.Order_date::date between '2024-01-01' and now()::date
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
with extra_invoice as(
	select
		invoice_no,
		sum(invoice_amz) invoice_amount
	from
		y4a_analyst.di_payment_amz_snapshot
	where
		UPPER(type_of_invoice) = 'EXTRA INVOICE' or invoice_vendor = 0
	group by
		1)
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
	case when b.invoice_no is null then wh_location else null end as "location" ,
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
	case when b.invoice_no is not null then 2 else 0 end is_processed
from
	y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp a
	left join extra_invoice b on a.external_doc_no = b.invoice_no;
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
with extra_invoice as(
	select
		invoice_no,
		sum(invoice_amz) invoice_amount
	from
		y4a_analyst.di_payment_amz_snapshot
	where
		UPPER(type_of_invoice) = 'EXTRA INVOICE' or invoice_vendor = 0
	group by
		1)
	select
	distinct external_doc_no as document_no,
	line_no ,
--Với Extra Invoice (DI Invoice nhưng không có mua hàng), Doanh thu sẽ được tính
--Trong doanh thu khác, không phải gross sales
	case when b.invoice_no is null then "type" else 'G/L Account' end as "type",
	case when b.invoice_no is null then "no" else '71190101' end as "no" ,
	description ,
	case when b.invoice_no is null then uom else null end as uom ,
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
	y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp  a
left join extra_invoice b on a.external_doc_no = b.invoice_no ;
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
	case when a.belong_to_company = 'HMD' then null else '156-OBH-CA' end as wh_location,
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
		and (a.belong_to_company not in ('Y4A', 'Hoang Thong','JTO') and a.belong_to_company is not null)
		and a.Order_date::date between '2024-01-01' and  now()::date
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
	'pcs' as uom ,	
	0 unit_price ,
	line_discount_pct ,
	0 discount ,
	quantity,
	wh_location location_code,
	vat_product_posting_group ,
	0 as amount,
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
where belong_to_company not in ('HMD');
update y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily set uom = null where "no" in ('33890101','71190101','52120101');
--update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
--set posting_date = (select max(to_char(posting_date,'YYYY-MM-DD'))::date from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily)
--where to_char(posting_date,'YYYY-MM') != (select max(to_char(posting_date,'YYYY-MM')) from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily);
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set is_processed = 0 where is_processed is null ;
-- 2024 changes--
-- All to 1311-MER
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set customer_posting_group = '1311-MER';
---Warehouse Location
--header
with upd_lct_header as (
with shp as (
select
*
, unnest(string_to_array(purchase_order::text, ','::text)) as un_purchase_order,
row_number() over (partition by y4a_dwc_amz_avc_shm_sum.shipment_id order by y4a_dwc_amz_avc_shm_sum.run_date desc) rn
from y4a_cdm.y4a_dwc_amz_avc_shm_sum
-- where status in ('COMPLETE','DELAYED_DELIVERY','DELAYED_PICKUP','IN_TRANSIT')
)
select distinct concat(un_purchase_order,'_',reference_id) shp_external_doc_no , ship_from_city from shp where rn= 1
)
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set "location" = (
case when external_doc_no in (select distinct shp_external_doc_no from upd_lct_header where ship_from_city = 'Fontana') then '156-3PL-FT'
	 when external_doc_no in (select distinct shp_external_doc_no from upd_lct_header where ship_from_city = 'Cranbury') then '156-3PL-CB'
	 when external_doc_no in (select distinct shp_external_doc_no from upd_lct_header where ship_from_city in ('Anaheim', 'Brea')) then '156-USA-CA'
	 when external_doc_no in (select distinct shp_external_doc_no from upd_lct_header where ship_from_city in ('FLORENCE')) then '156-3PL-FR'
	 else 'Unidentified'
end
)
where external_doc_no in (select distinct shp_external_doc_no from upd_lct_header where ship_from_city in ('Anaheim', 'Brea','Fontana','Cranbury','FLORENCE'));
--update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
--set "location" = '156-3PL-FT' where external_doc_no in (select distinct external_doc_no from y4a_erp.view_y4a_erp_sales_invoice_3pl_location_checking_daily where ship_from_city = 'Fontana') and "location" = 'Unidentified';
--update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
--set "location" = '156-3PL-CB' where external_doc_no in (select distinct external_doc_no from y4a_erp.view_y4a_erp_sales_invoice_3pl_location_checking_daily where ship_from_city = 'Cranbury')and "location" = 'Unidentified';
--update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
--set "location" = '156-USA-CA' where external_doc_no in (select distinct external_doc_no from y4a_erp.view_y4a_erp_sales_invoice_3pl_location_checking_daily where ship_from_city in ('Anaheim', 'Brea'))and "location" = 'Unidentified';
--
----line
with upd_lct_line as (
with shp as(select
*, unnest(string_to_array(purchase_order::text, ','::text)) as un_purchase_order,
row_number() over (partition by y4a_dwc_amz_avc_shm_sum.shipment_id order by y4a_dwc_amz_avc_shm_sum.run_date desc) rn
from y4a_cdm.y4a_dwc_amz_avc_shm_sum
-- where status in ('COMPLETE','DELAYED_DELIVERY','DELAYED_PICKUP','IN_TRANSIT')
)
select distinct concat(un_purchase_order,'_',reference_id) shp_external_doc_no , ship_from_city from shp where rn= 1 )
update y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily
set location_code = (
case when document_no in (select distinct shp_external_doc_no from upd_lct_line where ship_from_city = 'Fontana') then '156-3PL-FT'
	 when document_no in (select distinct shp_external_doc_no from upd_lct_line where ship_from_city = 'Cranbury') then '156-3PL-CB'
	 when document_no in (select distinct shp_external_doc_no from upd_lct_line where ship_from_city in ('Anaheim', 'Brea')) then '156-USA-CA'
	 when document_no in (select distinct shp_external_doc_no from upd_lct_line where ship_from_city in ('FLORENCE')) then '156-3PL-FR'
	 else 'Unidentified'
end
)
where document_no in (select distinct shp_external_doc_no from upd_lct_line where ship_from_city in ('Anaheim', 'Brea','Fontana','Cranbury','FLORENCE'));
--update y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily
--set location_code = '156-3PL-FT' where document_no in (select distinct external_doc_no from y4a_erp.view_y4a_erp_sales_invoice_3pl_location_checking_daily where ship_from_city = 'Fontana') and location_code = 'Unidentified';
--update y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily
--set location_code = '156-3PL-CB' where document_no in (select distinct external_doc_no from y4a_erp.view_y4a_erp_sales_invoice_3pl_location_checking_daily where ship_from_city = 'Cranbury') and location_code = 'Unidentified';
--update y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily
--set location_code = '156-USA-CA' where document_no in (select distinct external_doc_no from y4a_erp.view_y4a_erp_sales_invoice_3pl_location_checking_daily where ship_from_city in ('Anaheim', 'Brea')) and location_code = 'Unidentified';
---UPDATE LOCATION DI OBH ---
--header
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set "location" = '157-OBH' where belong_to_company not in ('Y4A','Hoang Thong','JTO') and internal_sales_channel = 'AVC DI';
--line
update y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily
set location_code = '157-OBH' where document_no in (select distinct external_doc_no from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily where belong_to_company not in ('Y4A','Hoang Thong','JTO') and internal_sales_channel = 'AVC DI') ;
---update name OBH
--update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
--set name = '331US-MER0019-NGU' where belong_to_company = 'General Union';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = 'US-MER0002-BLS' where belong_to_company = 'Bluestars';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = 'US-SER0002-BTT' where belong_to_company = 'Bitis';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = '331US-SER0019-A&D' where belong_to_company = 'A&D';
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set name = 'US-SER0003-HUR' where belong_to_company = 'HMD';
---Lấy ra các SI DI ko có Commercial Invoice, gửi cho AC để quyết định xem invoice nào sẽ đẩy vào ERP
--delete from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily where external_doc_no in (
--select distinct external_doc_no from y4a_erp.y4a_erp_view_avc_di_wh_si_not_having_pi a
--where 1=1
--);
--delete from y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily where document_no in (
--select distinct external_doc_no from y4a_erp.y4a_erp_view_avc_di_wh_si_not_having_pi a
--where 1=1
--);
	END;
$procedure$
;

