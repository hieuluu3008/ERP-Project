-- DROP PROCEDURE y4a_erp.sp_prod_sales_invoice_02_2_avc_ds_daily();
CREATE OR REPLACE PROCEDURE y4a_erp.sp_prod_sales_invoice_02_2_avc_ds_daily()
LANGUAGE plpgsql
AS $procedure$
	begin
	
--Daily load cho snapshot log
insert
	into
	y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log
-- AVC DS
(with avc_ds as(
select
	e.invoicedate ,
	a.invoicenumber ,
	c.customer_posting_group,
	'CHAN-AVC' sales_channel,
	'60D' payment_term,
	c.belong_to_company belong_to_company,
	pims.pims_sku,
	round((a.subtotal / a.quantity)::numeric, 2) unit_price,
	a.quantity ,
	round(a.subtotal::numeric, 2) amount,
	a.invoicenumber po_number,
	'AVC DS' internal_sales_channel,
	e.country,
	a.asin,
	comp.company_id y4a_company_id,
	'Y4ALLC' original_y4a_company_id
	, e.warehousecode
from
	y4a_cdm.y4a_dwc_amz_avc_dsi_dtl_itm a
left join y4a_cdm.y4a_dwc_amz_avc_dsi_dtl_sum e on
	a.invoicenumber = e.invoicenumber
left join y4a_finance.dim_pims_platform_product_profile_by_country pims on
	a.asin = pims.platform_prd_id
	and pims.country = e.country
left join y4a_erp.y4a_erp_dim_y4a_company_id comp on
	comp.company_code = 'Y4ALLC'
left join ( SELECT DISTINCT y4a_erp_prod_master_data_item.item_id,
           y4a_erp_prod_master_data_item.original_y4a_company_id,
           y4a_erp_prod_master_data_item.belong_to_company, customer_posting_group
          FROM y4a_erp.y4a_erp_prod_master_data_item) c on
	pims.pims_sku = c.item_id
	and c.original_y4a_company_id = 'Y4ALLC'
where
	e.invoicedate::date between '2024-06-01' and now()::date
	and e.status != 'REJECTED')
select
	a.invoicedate::date Order_date,
	null::date posting_date,
	a.invoicedate::date document_date,
	'Order' document_type,
	'131US-MER0001-AMZ' bill_to_customer,
	'131US-MER0001-AMZ' sell_to_customer,
	'PLF-AMZ-1' platform,
	customer_posting_group,
	null posting_description,
	payment_term,
	case
		when a.belong_to_company in ('Y4A', 'Hoang Thong','General Union','JTO') then a.invoicenumber
		when (a.belong_to_company not in ('Y4A', 'Hoang Thong','General Union','JTO')
			and a.belong_to_company is not null) then concat(a.invoicenumber, '-OBH-', a.belong_to_company)
	end external_doc_no,
	case
		when a.belong_to_company in ('Y4A', 'Hoang Thong','General Union','JTO') then 'Unidentified'
		when UPPER(a.belong_to_company) in ('HMD', 'MOONKEY') then null
		else '156-OBH-CA'
	end wh_location,
	null currency,
	'DATAMART_SI' source_code,
	'CHAN-AVC' sales_channel,
	CASE
		WHEN country = 'USA' THEN 'REG-USA'
		WHEN country = 'CAN' THEN 'REG-CAN'
	END as country,
	row_number() over(partition by case
		when a.belong_to_company in ('Y4A', 'Hoang Thong','General Union','JTO') then a.invoicenumber
		else concat(a.invoicenumber, '-OBH-', a.belong_to_company)
	end)* 10000 line_no,
	'ITEM' "type",
	pims_sku as "no",
	null description,
	'pcs' uom,
	unit_price unit_price,
	null::numeric line_discount_pct,
	null::numeric discount,
	a.quantity,
	'VAT-NOT' vat_product_posting_group,
	amount,
	null::numeric vat_amount,
	null batch_seq,
	po_number,
	true is_valid_record ,
	-- loại bỏ toàn bộ invoice line nếu không thỏa điều kiện
	now() data_updated_time,
	'AVC DS' internal_sales_channel,
	a.invoicenumber original_external_doc_no,
	asin,
	a.y4a_company_id,
	a.original_y4a_company_id,
	a.belong_to_company
from
	avc_ds a);
--left join (select distinct invoicenumber from y4a_cdm.y4a_dwc_amz_avc_dsi_dtl_sum where warehousecode = 'FBRV') b on a.invoicenumber = b.invoicenumber --Cranbury
--left join (select distinct invoicenumber from y4a_cdm.y4a_dwc_amz_avc_dsi_dtl_sum where warehousecode = 'FBBQ') c on a.invoicenumber = c.invoicenumber -- Fortanna
--left join (select distinct invoicenumber from y4a_cdm.y4a_dwc_amz_avc_dsi_dtl_sum where warehousecode in ('AUYA','ESDW')) d on a.invoicenumber = d.invoicenumber) -- Brea, Anaheim
--------------------------------------------------Xử lý giao dịch của Y4A, Hoang Thong-------------------------------------
--Daily load cho bảng temp
delete from y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp where 1=1;
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
select
	a.Order_date ,
--    a.Order_date::date as posting_date ,
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
	left join (select distinct original_external_doc_no from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental where document_type = 'Order' and internal_sales_channel = 'AVC DS') c on a.original_external_doc_no = c.original_external_doc_no
where
	a.data_updated_time = (
	select
		max(data_updated_time)
	from
		y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log)
		and a.is_valid_record = true
		and c.original_external_doc_no is null
		and a.belong_to_company in ('Y4A', 'Hoang Thong','General Union','JTO')
		and a.Order_date::date >= '2024-01-01' and a.Order_date <= now()::date
		;
-- Update Location
	
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
		"transaction" = 'AVC DS'
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
-- Update country
--update y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp a
--set country = 'REG-CAN'
--where wh_location = '156-3PL-CA';
	
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
	y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp a;
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
	y4a_erp.y4a_erp_prod_sales_invoice_incremental_temp  a;
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
--	a.Order_date::date posting_date ,
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
	left join (select distinct original_external_doc_no from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental where document_type = 'Order' and internal_sales_channel = 'AVC DS') c on a.original_external_doc_no = c.original_external_doc_no
where
	a.data_updated_time = (
	select
		max(data_updated_time)
	from
		y4a_erp.y4a_erp_prod_sales_invoice_snapshot_log)
		and a.is_valid_record = true
		and c.original_external_doc_no is null
		and (a.belong_to_company not in ('Y4A', 'Hoang Thong','General Union','JTO') and a.belong_to_company is not null)
		and a.Order_date::date >= '2024-01-01' and a.Order_date <= now()::date		;
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
	is_processed ,
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
	0,
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
---Daily load cho sales invoice line OBH tang ton`
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
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily a
set country = 'REG-CAN'
where "location" = '156-3PL-CA';
update y4a_erp.y4a_erp_prod_sales_invoice_line_incremental_daily a
set country = 'REG-CAN'
where location_code = '156-3PL-CA';
delete from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
where external_doc_no in (
		select distinct external_doc_no from (
	select external_doc_no, string_agg(distinct belong_to_company,', ') as belong_to_company_list
	from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily group by 1
	having string_agg(distinct belong_to_company,', ') like '%,%'
		and (string_agg(distinct belong_to_company,', ') like '%Hoang Thong%'
			or string_agg(distinct belong_to_company,', ') like '%General Union%'
			or string_agg(distinct belong_to_company,', ') like '%JTO%'
			)
		and string_agg(distinct belong_to_company,', ') like '%Y4A%') z
	) and belong_to_company in ('Hoang Thong','General Union','JTO');
---FROM 2024-1-1
update y4a_erp.y4a_erp_prod_sales_invoice_header_incremental_daily
set customer_posting_group = '1311-MER';
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
	END;
$procedure$
;

