-- DROP PROCEDURE y4a_erp.sp_y4a_erp_selling_expense_acr_est_dtl();
CREATE OR REPLACE PROCEDURE y4a_erp.sp_y4a_erp_selling_expense_acr_est_dtl()
LANGUAGE plpgsql
AS $procedure$
	BEGIN
		---- Lưu ý qua đến tháng sau tháng closing mới chạy data, nếu không phải check lại khung thời gian lấy posting_date
TRUNCATE TABLE
	y4a_erp.tb_y4a_erp_selling_expense_acr_est_dtl;
INSERT
	INTO
	y4a_erp.tb_y4a_erp_selling_expense_acr_est_dtl
	(
WITH cte_acc AS (
   WITH cte_inv AS (
         with cte_inv_si as (
		            SELECT
		                t1.document_no,
		                t1.po_number,
		                t2.posting_date,
		                t2.document_date,
		                t2.country,
		                t2.currency,
		                t1.no AS sku,
		                CASE WHEN t2.belong_to_company IN ('Hoang Thong','JTO','General Union') THEN 'Y4A' ELSE t2.belong_to_company END AS company,
		                t1.unit_price,
		                t1.quantity
		                ,
		                CASE
						    WHEN t2.sales_channel IN ('CHAN-WAYFAIR', 'CHAN-WMDSV') THEN t1.quantity * t1.unit_price
						    ELSE t1.amount
						END AS amount,
		                t1.sales_channel,
		                t1.platform,
		                t2.internal_sales_channel,
		                t2.is_processed,
		                t2.original_y4a_company_id,
		                CASE
		                	WHEN t2.posting_description ~ 'Booking accrual revenue' THEN 'SI Accrual'
		                	ELSE 'SI Actual'
		                END "type"
		            FROM
		                y4a_erp.y4a_erp_prod_sales_invoice_line_incremental t1
		                LEFT JOIN y4a_erp.y4a_erp_prod_sales_invoice_header_incremental t2
		                ON t1.document_no = t2.external_doc_no
		                WHERE
		                t2.internal_sales_channel = ANY (
		                    ARRAY ['AVC DS'::text, 'AVC WH'::text, 'AVC DI'::text,'AVC GPE','WF'::TEXT,'WF CG'::text]
		                )
		                AND t2.is_processed =1
						AND t2.bill_to_customer !~ 'INTC'
		                and t2.posting_date  >= date_trunc('month',y4a_erp.end_of_last_month(now()::date))::date
						AND t2.posting_date  < date_trunc('month',now()::date)::date
						AND upper(t2.document_type) = 'ORDER'
						AND t2.country = ANY (ARRAY['REG-DEU', 'REG-GBR', 'REG-JPN', 'REG-USA', 'REG-ITA', 'REG-FRA', 'REG-ESP', 'REG-MXN','REG-AUS','REG-CAN'])
       ),
       cte_inv_rtn AS
       (
			        	WITH
						   	get_int_sc AS
						   	(
						 --Mục đích để xác định kênh DI hay kênh WH
						  select
						    poid po,
						    case
						      when vendor in ('YES4A', 'YES4M') then 'AVC WH'
						      when vendor in ('YEX5L') then 'AVC GPE'
						      when vendor is null then 'Unidentified'
						      else 'AVC DI'
						    end sales_channel
						  from
						    y4a_cdm.y4a_dwc_amz_avc_pco_sum
						  group by
						    1,
						    2
							), srt_dswh_inv AS
							(
							select
						    t1.document_no,
						    t1.po_number,
						    t2.posting_date,
						    t2.document_date,
						    t1.country,
						    t2.currency,
						    t1.no AS sku,
						    CASE WHEN t2.belong_to_company IN ('Hoang Thong','JTO','General Union')  THEN 'Y4A' ELSE t2.belong_to_company END  AS company,
						    t1.unit_price,
						    t1.quantity,
						    - t1.amount as amount,
						    t1.sales_channel,
						    t1.platform,
						    CASE WHEN po_number ~'DROPSHIP' THEN 'AVC DS' ELSE 'AVC WH' END AS internal_sales_channel,
						    t2.is_processed,
						    t2.original_y4a_company_id,
						    'SRT Actual' "type"
						FROM
						    y4a_erp.y4a_erp_prod_sales_invoice_line_incremental t1
						    LEFT JOIN  y4a_erp.y4a_erp_prod_sales_invoice_header_incremental t2
							ON t1.document_no = t2.external_doc_no
						    LEFT JOIN get_int_sc t4
						    ON t1.po_number = t4.po 
						     WHERE t2.document_type ='Sales Return'
						     AND t2.is_processed =1
							 and t2.posting_date  >= date_trunc('month',y4a_erp.end_of_last_month(now()::date))::date
							 AND t2.posting_date  < date_trunc('month',now()::date)::date
						     AND (t4.sales_channel IN ('AVC DS','AVC WH') OR t4.sales_channel IS NULL)
						     AND t1.sales_channel ='CHAN-AVC'
						     AND t2.bill_to_customer not in ('136INTC-PREP','136INTC-IDZ')
						     ),
						     srt_di_inv AS (
						select
						    t1.document_no,
						    t1.po_number,
						    t2.posting_date,
						    t2.document_date,
						    t1.country,
						    t2.currency,
						    t1.no AS sku,
						    CASE WHEN t2.belong_to_company  IN ('Hoang Thong','JTO','General Union') THEN 'Y4A' ELSE t2.belong_to_company END AS company,
						    t1.unit_price,
						    t1.quantity,
						    - t1.amount as amount,
						    t1.sales_channel,
						    t1.platform,
						    t4.sales_channel AS internal_sales_channel,
						    t2.is_processed,
						    t2.original_y4a_company_id,
						    'SRT Actual' "type"
						FROM
						    y4a_erp.y4a_erp_prod_sales_invoice_line_incremental t1
						    LEFT JOIN  y4a_erp.y4a_erp_prod_sales_invoice_header_incremental t2
							ON t1.document_no = t2.external_doc_no
						    LEFT JOIN get_int_sc t4
						    ON t1.po_number = t4.po 
						     WHERE t2.document_type ='Sales Return'
						     AND t2.is_processed =1
							 and t2.posting_date  >= date_trunc('month',y4a_erp.end_of_last_month(now()::date))::date
							 AND t2.posting_date  < date_trunc('month',now()::date)::date
						     AND t4.sales_channel = 'AVC DI'
						     AND po_number != 'DROPSHIP-PO-AUYA-US'
						     AND t1.sales_channel ='CHAN-AVC'
						     AND split_part(t1.country,'-',2) IN ('DEU','JPN','GBR','ITA','FRA','ESP','MXN','AUS','CAN')
						     AND t2.bill_to_customer not in ('136INTC-PREP','136INTC-IDZ')						  
						         )
			     SELECT * FROM srt_dswh_inv
			     UNION ALL
			     SELECT * FROM srt_di_inv
			     )
       SELECT * from
           cte_inv_si
       UNION ALL
       SELECT * from
           cte_inv_rtn
   ),
   cte_rate AS (
       SELECT
       	cte_inv.posting_date,
           replace(cte_inv.document_no, '_Return', '') as document_no,
           cte_inv.po_number,
           cte_inv.document_date,
           cte_inv.country,
           cte_inv.currency,
           cte_inv.sku,
           cte_inv.company,
           original_y4a_company_id,
           cte_inv.unit_price,
           cte_inv.quantity,
           cte_inv.amount,
           cte_inv.sales_channel,
           cte_inv.platform,
           is_processed,
           cte_inv.internal_sales_channel,
           "type",
           CASE
               WHEN cte_inv.internal_sales_channel = ANY (ARRAY ['AVC DS'::text, 'AVC WH'::text]) THEN 0.09
               ELSE 0 :: numeric
           END AS coop_mkt_rate,
           CASE
               WHEN document_no not like '%Return%'
               and cte_inv.internal_sales_channel = 'AVC WH' :: text THEN CASE
                   WHEN cte_inv.document_date <= '2023-03-31' :: date THEN 0.117
                   ELSE 0.122
               END
               ELSE 0 :: numeric
           END AS freight_allowance_rate,
           CASE
               WHEN cte_inv.internal_sales_channel = 'AVC DI' :: text
               AND cte_inv.country in ('REG-DEU','REG-GBR','REG-ITA','REG-FRA','REG-ESP') THEN 0.02
               ELSE 0 :: numeric
           END AS damage_rate,
           CASE
               WHEN cte_inv.internal_sales_channel = 'AVC DI' :: text
               AND cte_inv.country = 'REG-JPN' :: text THEN 0.08
               ELSE 0 :: numeric
           END AS jpn_avs_svs_rate,
           CASE
	            WHEN  cte_inv.internal_sales_channel = 'AVC DI' :: text
           	AND cte_inv.country in ('REG-DEU','REG-GBR','REG-AUS') THEN 0.03
               ELSE 0 :: numeric
           END AS di_avs_svs_rate,
           CASE
	            WHEN  cte_inv.internal_sales_channel = 'AVC DI' :: text
           	AND cte_inv.country in ('REG-DEU','REG-GBR','REG-ITA','REG-FRA','REG-ESP','REG-MXN') THEN 0.02
               ELSE 0 :: numeric
           END AS di_coop_rate,
           CASE
               WHEN cte_inv.internal_sales_channel IN ('WF','WF CG') THEN 0.04
               ELSE 0 :: numeric
           END AS wf_damage_rate
           ,CASE
               WHEN cte_inv.internal_sales_channel = ANY (ARRAY ['AVC DS'::text, 'AVC WH'::text])
--                and (b.department = 'SSO' or (cte_inv.sku = '33890101' and cte_inv.company = 'A&D') or cte_inv.document_no = 'Qt0tT4NTp')
               and rtn.external_doc_no is null then 0.0375
               ELSE 0 :: numeric
           END AS sg_damage_rate
			,CASE
				WHEN cte_inv.internal_sales_channel IN ('AVC DS', 'AVC DI', 'AVC WH')
					AND cte_inv.sku IN (
						SELECT DISTINCT pims_sku
						FROM y4a_sale.y4a_avc_report_retail_analytics_catalog a
						LEFT JOIN y4a_finance.dim_pims_platform_product_profile_by_country b ON a.asin = b.platform_prd_id
						WHERE 1=1
						AND product_group = 'Furniture'
						AND pims_sku IS NOT NULL )
				THEN 0.013
				ELSE 0::numeric
			END AS svs_avs_fur
       FROM
           cte_inv
--        left join (select distinct dst.platform, left(dst.sku, 4) sku, dst.country, department from
--					y4a_finance.dim_pims_product_profile sku
--					left join y4a_finance.dim_sale_team_by_country_sku_platform dst on sku.sku = dst.sku
--					where department = 'SSO') b
--			on split_part(cte_inv.country,'-',2) = b.country and cte_inv.sku = b.sku and split_part(cte_inv.platform,'-',2) = b.platform
		left join (select distinct replace(external_doc_no, '_Return', '') external_doc_no, document_type from y4a_erp.y4a_erp_prod_sales_invoice_header_incremental where document_type = 'Sales Return') rtn
			on replace(cte_inv.document_no, '_Return', '') = rtn.external_doc_no
   )
   SELECT
   	cte_rate.posting_date,
       cte_rate.sales_channel,
       cte_rate.internal_sales_channel,
       cte_rate.platform,
       cte_rate.document_no AS invoice_number,
       cte_rate.po_number,
       cte_rate.country,
       cte_rate.currency,
       cte_rate.document_date,
       cte_rate.sku,
       cte_rate.company,
       original_y4a_company_id,
       "type",
       cte_rate.unit_price,
       cte_rate.quantity,
       cte_rate.amount,
       is_processed,
       cte_rate.coop_mkt_rate,
       cte_rate.coop_mkt_rate * cte_rate.amount AS coop_mkt_amt,
       cte_rate.freight_allowance_rate,
       cte_rate.freight_allowance_rate * cte_rate.amount AS freight_allowance_amt,
       cte_rate.damage_rate,
       cte_rate.damage_rate * cte_rate.amount AS damage_amt,
       cte_rate.jpn_avs_svs_rate,
       cte_rate.jpn_avs_svs_rate * cte_rate.amount AS jpn_avs_svs_amt,
       cte_rate.di_avs_svs_rate,
       cte_rate.di_avs_svs_rate * cte_rate.amount AS di_avs_svs_amt,
       cte_rate.di_coop_rate,
       cte_rate.di_coop_rate * cte_rate.amount AS di_coop_amt,     
       cte_rate.wf_damage_rate,
       cte_rate.wf_damage_rate * cte_rate.amount AS wf_damage_amt
       ,cte_rate.sg_damage_rate,
       cte_rate.sg_damage_rate * cte_rate.amount AS sg_damage_amt
		,cte_rate.svs_avs_fur,
		cte_rate.svs_avs_fur * cte_rate.amount AS svs_avs_fur_amt
   FROM
       cte_rate
)
SELECT
	cte_acc.posting_date,
   cte_acc.sales_channel,
   cte_acc.internal_sales_channel,
   cte_acc.platform,
   concat(cte_acc.invoice_number, '_', company) as invoice_number,
   cte_acc.po_number,
   cte_acc.country,
   cte_acc.currency,
   cte_acc.company,
   original_y4a_company_id,
   cte_acc.document_date,
   cte_acc.sku,
   cte_acc.unit_price,
   cte_acc.quantity,
   cte_acc.amount,
   is_processed,
   cte_acc.coop_mkt_rate AS rate,
    cte_acc.coop_mkt_amt AS accrual_amt,
    cte_acc.invoice_number as original_invoice,
   'COOP_MKT' :: text AS note,
   "type"
FROM
   cte_acc
WHERE
   cte_acc.coop_mkt_amt <> 0 :: NUMERIC
   UNION
ALL
SELECT
	cte_acc.posting_date,
   cte_acc.sales_channel,
   cte_acc.internal_sales_channel,
   cte_acc.platform,
   concat(cte_acc.invoice_number, '_', company) as invoice_number,
   cte_acc.po_number,
   cte_acc.country,
   cte_acc.currency,
   cte_acc.company,
   original_y4a_company_id,
   cte_acc.document_date,
   cte_acc.sku,
   cte_acc.unit_price,
   cte_acc.quantity,
   cte_acc.amount,
   is_processed,
   cte_acc.di_coop_rate AS rate,
   cte_acc.di_coop_amt AS accrual_amt,
   cte_acc.invoice_number as original_invoice,
   'Coop DI' :: text AS note,
   "type"
FROM
   cte_acc
WHERE
   cte_acc.di_coop_amt <> 0 :: NUMERIC
       UNION
ALL
SELECT
	cte_acc.posting_date,
   cte_acc.sales_channel,
   cte_acc.internal_sales_channel,
   cte_acc.platform,
   concat(cte_acc.invoice_number, '_', company) as invoice_number,
   cte_acc.po_number,
   cte_acc.country,
   cte_acc.currency,
   cte_acc.company,
   original_y4a_company_id,
   cte_acc.document_date,
   cte_acc.sku,
   cte_acc.unit_price,
   cte_acc.quantity,
   cte_acc.amount,
   is_processed,
   cte_acc.jpn_avs_svs_rate AS rate,
   cte_acc.jpn_avs_svs_amt AS accrual_amt,
   cte_acc.invoice_number as original_invoice,
   'DI JPN AVS_SVS' :: text AS note,
   "type"
FROM
   cte_acc
WHERE
   cte_acc.jpn_avs_svs_amt <> 0 :: NUMERIC
           UNION
ALL
SELECT
	cte_acc.posting_date,
   cte_acc.sales_channel,
   cte_acc.internal_sales_channel,
   cte_acc.platform,
   concat(cte_acc.invoice_number, '_', company) as invoice_number,
   cte_acc.po_number,
   cte_acc.country,
   cte_acc.currency,
   cte_acc.company,
   original_y4a_company_id,
   cte_acc.document_date,
   cte_acc.sku,
   cte_acc.unit_price,
   cte_acc.quantity,
   cte_acc.amount,
   is_processed,
   cte_acc.di_avs_svs_rate AS rate,
   cte_acc.di_avs_svs_amt AS accrual_amt,
   cte_acc.invoice_number as original_invoice,
   'DI DEU GBR AVS_SVS' :: text AS note,
   "type"
FROM
   cte_acc
WHERE
   cte_acc.di_avs_svs_rate <> 0 :: numeric
UNION
ALL
SELECT
	cte_acc.posting_date,
   cte_acc.sales_channel,
   cte_acc.internal_sales_channel,
   cte_acc.platform,
   concat(cte_acc.invoice_number, '_', company) as invoice_number,
   cte_acc.po_number,
   cte_acc.country,
   cte_acc.currency,
   cte_acc.company,
   original_y4a_company_id,
   cte_acc.document_date,
   cte_acc.sku,
   cte_acc.unit_price,
   cte_acc.quantity,
   cte_acc.amount,
   is_processed,
   cte_acc.freight_allowance_rate AS rate,
   cte_acc.freight_allowance_amt AS accrual_amt,
   cte_acc.invoice_number as original_invoice,
   'Freight_Allowance' :: text AS note,
   "type"
FROM
   cte_acc
WHERE
   cte_acc.freight_allowance_amt <> 0 :: numeric
UNION
ALL
SELECT
	cte_acc.posting_date,
   cte_acc.sales_channel,
   cte_acc.internal_sales_channel,
   cte_acc.platform,
   concat(cte_acc.invoice_number, '_', company) as invoice_number,
   cte_acc.po_number,
   cte_acc.country,
   cte_acc.currency,
   cte_acc.company,
   original_y4a_company_id,
   cte_acc.document_date,
   cte_acc.sku,
   cte_acc.unit_price,
   cte_acc.quantity,
   cte_acc.amount,
   is_processed,
   cte_acc.damage_rate AS rate,
   cte_acc.damage_amt AS accrual_amt,
   cte_acc.invoice_number as original_invoice,
   'Damage_Allowance' :: text AS note,
   "type"
FROM
   cte_acc
WHERE
   cte_acc.damage_rate <> 0 :: numeric
UNION
ALL
SELECT
	cte_acc.posting_date,
   cte_acc.sales_channel,
   cte_acc.internal_sales_channel,
   cte_acc.platform,
   concat(cte_acc.invoice_number, '_', company) as invoice_number,
   cte_acc.po_number,
   cte_acc.country,
   cte_acc.currency,
   cte_acc.company,
   original_y4a_company_id,
   cte_acc.document_date,
   cte_acc.sku,
   cte_acc.unit_price,
   cte_acc.quantity,
   cte_acc.amount,
   is_processed,
   cte_acc.wf_damage_rate AS rate,
   cte_acc.wf_damage_amt AS accrual_amt,
   cte_acc.invoice_number as original_invoice,
   'Damage_Allowance' :: text AS note,
   "type"
FROM
   cte_acc
WHERE
   cte_acc.wf_damage_amt <> 0 :: numeric
UNION
ALL
SELECT
	cte_acc.posting_date,
   cte_acc.sales_channel,
   cte_acc.internal_sales_channel,
   cte_acc.platform,
   concat(cte_acc.invoice_number, '_', company) as invoice_number,
   cte_acc.po_number,
   cte_acc.country,
   cte_acc.currency,
   cte_acc.company,
   original_y4a_company_id,
   cte_acc.document_date,
   cte_acc.sku,
   cte_acc.unit_price,
   cte_acc.quantity,
   cte_acc.amount,
   is_processed,
   cte_acc.sg_damage_rate AS rate,
   cte_acc.sg_damage_amt AS accrual_amt,
   cte_acc.invoice_number as original_invoice,
   'Damage_Allowance' :: text AS note,
   "type"
FROM
   cte_acc
WHERE
   cte_acc.sg_damage_amt <> 0 :: NUMERIC
UNION
ALL
SELECT
	cte_acc.posting_date,
   cte_acc.sales_channel,
   cte_acc.internal_sales_channel,
   cte_acc.platform,
   concat(cte_acc.invoice_number, '_', company) as invoice_number,
   cte_acc.po_number,
   cte_acc.country,
   cte_acc.currency,
   cte_acc.company,
   original_y4a_company_id,
   cte_acc.document_date,
   cte_acc.sku,
   cte_acc.unit_price,
   cte_acc.quantity,
   cte_acc.amount,
   is_processed,
   cte_acc.svs_avs_fur AS rate,
   cte_acc.svs_avs_fur_amt AS accrual_amt,
   cte_acc.invoice_number as original_invoice,
   'Furniture_SVS_AVS' :: text AS note,
   "type"
FROM
   cte_acc
WHERE
   cte_acc.svs_avs_fur_amt <> 0 :: NUMERIC
 ) ;
	END;
$procedure$
;

