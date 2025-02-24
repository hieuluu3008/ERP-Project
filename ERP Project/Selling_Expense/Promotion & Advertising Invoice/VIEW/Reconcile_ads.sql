-- y4a_erp.view_check_ads_total_amount_erp source
CREATE OR REPLACE VIEW y4a_erp.view_check_ads_total_amount_erp
AS WITH ads_raw AS (
        SELECT c.accountinfo_name,
           sum(a_1.summary_amountdue_amount + a_1.summary_taxamountdue_amount) AS amount_raw
          FROM y4a_cdm.y4a_dwa_amz_ads_inv_dtl a_1
            LEFT JOIN y4a_cdm.y4a_dwa_amz_ads_inv b_1 ON a_1.summary_id::text = b_1.invoice_id::text
            LEFT JOIN ( SELECT y4a_dwa_amz_ads_profile_info.profileid,
                   y4a_dwa_amz_ads_profile_info.accountinfo_name,
                   y4a_dwa_amz_ads_profile_info.accountinfo_type
                  FROM y4a_cdm.y4a_dwa_amz_ads_profile_info) c ON b_1.profile_id::text = c.profileid::text
         WHERE b_1.invoicedate::date >= (date_trunc('month'::text, now()::date - '2 mons'::interval) + '24 days'::interval) AND b_1.invoicedate::date < (date_trunc('month'::text, now()::date - '5 days'::interval) + '25 days'::interval)
         GROUP BY c.accountinfo_name
       )
SELECT split_part(a.descriptions, ' '::text, 3) AS se_type,
   a.accounts,
   a.region_code,
   split_part(a.original_invoice_company, '_'::text, 2) AS split_part,
   sum(a.amount::numeric(10,2)) AS amount_erp,
   b.amount_raw,
   sum(a.amount::numeric(10,2)) - b.amount_raw AS reconcile
  FROM y4a_erp.y4a_erp_prod_selling_expense_incremental_daily a
    LEFT JOIN ads_raw b ON a.accounts::text = b.accountinfo_name
 WHERE a.document_date < (date_trunc('month'::text, now()::date - '5 days'::interval) + '25 days'::interval) AND a.document_date >= (date_trunc('month'::text, now()::date - '2 mons'::interval) + '24 days'::interval) AND a.amount > 0::numeric AND a.is_processed = 2 AND a.descriptions ~ 'Marketing'::text
 GROUP BY (split_part(a.descriptions, ' '::text, 3)), a.accounts, a.region_code, (split_part(a.original_invoice_company, '_'::text, 2)), b.amount_raw
 ORDER BY a.accounts;

