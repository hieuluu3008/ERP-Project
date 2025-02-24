-- DROP PROCEDURE y4a_erp.sp_y4a_erp_sel_exp_ads_dtl_api_upt();
CREATE OR REPLACE PROCEDURE y4a_erp.sp_y4a_erp_sel_exp_ads_dtl_api_upt()
LANGUAGE plpgsql
AS $procedure$
	BEGIN
		truncate table y4a_erp.tb_y4a_erp_sel_exp_ads_dtl_update;
	insert into y4a_erp.tb_y4a_erp_sel_exp_ads_dtl_update
with cte_ads_full as (
   WITH cte_ads_raw AS (
       SELECT
           t1.summary_id AS invoice_number,
           t1.summary_invoicedate :: date AS invoice_date,
           t2.profile_id AS acc_id,
           CASE WHEN summary_amountdue_currencycode ='SGD' THEN 'SGP' ELSE t4.country_code_3 END AS country,
           t1.summary_amountdue_currencycode AS currency,
           'nonASIN' :: text AS asin,
           'nonSKU' :: text AS sku,
           case
               when summary_status = 'WRITTEN_OFF'
               and summary_remainingamountdue_amount = 0 then 0
               else t1.summary_amountdue_amount + t1.summary_taxamountdue_amount
           end AS amount,
           CASE
               WHEN t3.channel = 'vendor' :: text THEN 'AVC' :: text
               WHEN t3.channel = 'seller' :: text THEN 'ASC' :: text
               ELSE NULL :: text
           END AS channel,
           'Amazon' :: text AS platform,
           t3.account as account_name,
           t3.original_y4a_company_id
       FROM
           y4a_cdm.y4a_dwa_amz_ads_inv_dtl t1
           LEFT JOIN y4a_cdm.y4a_dwa_amz_ads_inv t2 ON t1.summary_id :: text = t2.invoice_id :: text
           LEFT JOIN (
               select
                   distinct profileid,
                   accountinfo_type as channel,
                   accountinfo_name as account,
                   accountinfo_type as account_type,countrycode,
                   case
                       when accountinfo_type = 'vendor' then 'Y4ALLC'
                       when accountinfo_type = 'seller' then case
                           when upper(accountinfo_name) LIKE  '%A.I SALES%' then 'PR-ENTERPRISE'
                           when upper(accountinfo_name) LIKE  '%IDZO%' then 'IDZO'
                           else 'OB'
                       end
                   end as original_y4a_company_id
               from
                   y4a_cdm.y4a_dwa_amz_ads_profile_info
           ) t3 ON t2.profile_id :: text = t3.profileid :: TEXT
           LEFT JOIN y4a_finance.dim_country t4 ON COALESCE (t1.issuer_address_countrycode,t3.countrycode ) :: text = t4.country_code_2 :: TEXT
           WHERE t3.account NOT IN ('3PTestBrand-A21VBSXVNL5139419282815527')
       ORDER BY
           t1.summary_invoicedate
   ),
   cte_acc_name_ob as (
       select
           a.invoice_id,
           case
               when name like '%HMD%' then 'HMD'
               else 'Y4A'
           end as company,
           sum(totalamount_amount + COALESCE (b.amount_amount,0) - feeamount_amount) :: numeric(10, 2) as amt
--            sum(totalamount_amount) :: numeric(10, 2) as amt
       from
           y4a_cdm.Y4A_DWA_AMZ_ADS_INV_PRF a
           left join y4a_cdm.y4a_dwa_amz_ads_inv_dtl_adj b on a.invoice_id = b.invoice_id
    and a.portfolio_id = b.portfolioid
       where
           (case
               when name like '%HMD%' then 'HMD'
               else 'Y4A'
           end) != 'Y4A'
       group by
           1,
           2
   )
   select
       t1.*,
       coalesce (t2.amt, 0) as HMD_amt,
       t1.amount - coalesce (t2.amt, 0) as y4a_amt
   from
       cte_ads_raw t1
       left join cte_acc_name_ob t2 on t1.invoice_number = t2.invoice_id
),
cte_ads_y4a as (
   select
       invoice_number,
       invoice_date,
       acc_id,
       country,
       currency,
       asin,
       sku,
       CASE WHEN account_name ='Services - Bluestars' THEN 'Bluestars'
--        WHEN account_name LIKE 'Yes4All - Biti%' THEN 'Bitis'
       WHEN account_name LIKE 'Services - Vitox' THEN 'A&D'
       ELSE 'Y4A' END as company,
       y4a_amt as invoice_amount,
       channel,
       platform,
       account_name,
       original_y4a_company_id
   from
       cte_ads_full
),
cte_ads_ob as (
   select
       invoice_number,
       invoice_date,
       acc_id,
       country,
       currency,
       asin,
       sku,
       'HMD' as company,
       hmd_amt as invoice_amount,
       channel,
       platform,
       account_name,
       original_y4a_company_id
   from
       cte_ads_full
   where
       hmd_amt <> 0
),
cte_result as(
   select
       *
   from
       cte_ads_y4a
   union
   all
   select
       *
   from
       cte_ads_ob
)
select
   *
from
   cte_result
where
   invoice_date >= '2024-06-01'
UNION
ALL
SELECT
   t1.document_number AS invoice_number,
   t1.document_date :: date AS invoice_date,
   NULL :: character varying AS acc_id,
   t2.country_code_3 AS country,
   t1.currency,
   'nonASIN' :: text AS asin,
   'nonSKU' :: text AS sku,
   'Y4A' as company,
   t1.invoice_amount,
   'WM - DSV' :: text AS channel,
   'Walmart' :: text AS platform,
   'Walmart' as account_name,
   'Y4ALLC' as original_y4a_company_id
FROM
   y4a_cdm.y4a_dwb_wlm_cls_bil t1
   LEFT JOIN y4a_analyst.dim_country t2 ON t1.country_key :: text = t2.country_code_2 :: text
WHERE
   (
       t1.doc_type_desc :: text = ANY (
           ARRAY ['WIC - AR'::character varying::text, 'Walmart Connect SS'::character varying::text]
       )
   )
   and t1.document_date >= '2024-06-01'
   and t1.document_number not in ('7000739536')
UNION
all
SELECT
   t1.document_number AS invoice_number,
   t1.document_date :: date AS invoice_date,
   NULL :: character varying AS acc_id,
   t2.country_code_3 AS country,
   t1.currency,
   'nonASIN' :: text AS asin,
   'nonSKU' :: text AS sku,
   'Y4A' as company,
   t1.open_amount AS invoice_amount,
   'WM - DSV' :: text AS channel,
   'Walmart' :: text AS platform,
   'Walmart' as account_name,
   'Y4ALLC' as original_y4a_company_id
FROM
   y4a_cdm.y4a_dwb_wlm_ope_bil t1
   LEFT JOIN y4a_analyst.dim_country t2 ON t1.country_key :: text = t2.country_code_2 :: text
WHERE
   t1.reason_code_description = 'Advertising Billing'
   and t1.document_date >= '2024-06-01'
   -- hoa don 7000739536 da duoc ke toan book tu t5 nen loai ra khoi chi phi thang 6
   and t1.document_number not in ('7000739536')
UNION
ALL
SELECT
-- bat dau tu thang 7 logic doi thanh invoice_number = externalpaymentid
	externalpaymentid invoice_number,
   report_date :: date AS invoice_date,
   NULL  acc_id,
   CASE
		WHEN account_name = 'CAN_CAN_PRSEnterprisesLLC' THEN 'CAN'
		WHEN account_name = 'PRSEnterprisesLLC' THEN 'USA'
	END country,
   currencyid AS currency,
   'nonASIN' :: text AS asin,
   'nonSKU' :: text AS sku,
   'Y4A' as company,
   amount AS invoice_amount,
   'WF - DSV' :: text AS channel,
   'Wayfair' :: text AS platform,
   'Wayfair' as account_name,
   'PR-ENTERPRISE' as original_y4a_company_id
FROM
   y4a_cdm.y4a_dwb_wyf_ads_wlt
WHERE
   y4a_dwb_wyf_ads_wlt.status :: text = 'Paid' :: TEXT
   and report_date >= '2024-06-01'
  ;
	END;
$procedure$
;

