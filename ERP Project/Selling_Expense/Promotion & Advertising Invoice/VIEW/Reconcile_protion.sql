-- y4a_erp.view_check_total_amount_promo_cpn source
CREATE OR REPLACE VIEW y4a_erp.view_check_total_amount_promo_cpn
AS WITH erp AS (
        SELECT to_char(a_1.document_date::timestamp with time zone, 'YYYY-MM'::text) AS year_month,
           split_part(a_1.descriptions, ' '::text, 3) AS se_type,
           a_1.region_code,
           sum(a_1.amount::numeric(10,2)) AS erp_amount
          FROM y4a_erp.y4a_erp_prod_selling_expense_incremental_daily a_1
         WHERE a_1.document_date < (date_trunc('month'::text, now()::date - '5 days'::interval)::date + '25 days'::interval) AND a_1.document_date >= date_trunc('month'::text, (now() - '1 mon'::interval)::date::timestamp with time zone)::date AND a_1.amount > 0::numeric AND a_1.is_processed = 2 AND split_part(a_1.descriptions, ' '::text, 3) = 'Promotions'::text AND a_1.region_code::text !~ 'JPN'::text
         GROUP BY (split_part(a_1.descriptions, ' '::text, 3)), a_1.region_code, (to_char(a_1.document_date::timestamp with time zone, 'YYYY-MM'::text))
       UNION ALL
        SELECT to_char(a_1.document_date::timestamp with time zone, 'YYYY-MM'::text) AS year_month,
           split_part(a_1.descriptions, ' '::text, 3) AS se_type,
           a_1.region_code,
           sum(a_1.amount::numeric(10,2)) AS erp_amount
          FROM y4a_erp.y4a_erp_prod_selling_expense_incremental_daily a_1
         WHERE a_1.document_date < date_trunc('month'::text, (now() + '1 mon'::interval)::date::timestamp with time zone)::date AND a_1.document_date >= date_trunc('month'::text, (now() - '2 mons'::interval)::date::timestamp with time zone)::date AND a_1.amount > 0::numeric AND split_part(a_1.descriptions, ' '::text, 3) = 'Promotions'::text AND a_1.region_code::text ~ 'JPN'::text
         GROUP BY (split_part(a_1.descriptions, ' '::text, 3)), a_1.region_code, (to_char(a_1.document_date::timestamp with time zone, 'YYYY-MM'::text))
       ), raw AS (
        SELECT to_char(y4a_dwc_amz_cop_ovr.invoice_date::date::timestamp with time zone, 'YYYY-MM'::text) AS year_month,
           y4a_dwc_amz_cop_ovr.country,
           sum(y4a_dwc_amz_cop_ovr.original_balance) AS raw_amount
          FROM y4a_cdm.y4a_dwc_amz_cop_ovr
         WHERE y4a_dwc_amz_cop_ovr.funding_type::text = 'Vendor Funded Sales Discount'::text AND y4a_dwc_amz_cop_ovr.invoice_date::date < (date_trunc('month'::text, now()::date - '5 days'::interval)::date + '25 days'::interval) AND y4a_dwc_amz_cop_ovr.invoice_date::date >= date_trunc('month'::text, (now() - '1 mon'::interval)::date::timestamp with time zone)::date AND y4a_dwc_amz_cop_ovr.country::text <> 'JPN'::text
         GROUP BY (to_char(y4a_dwc_amz_cop_ovr.invoice_date::date::timestamp with time zone, 'YYYY-MM'::text)), y4a_dwc_amz_cop_ovr.country
       UNION ALL
        SELECT to_char(y4a_dwc_amz_cop_ovr.invoice_date, 'YYYY-MM'::text) AS year_month,
           y4a_dwc_amz_cop_ovr.country,
           sum(y4a_dwc_amz_cop_ovr.original_balance) AS raw_amount
          FROM y4a_cdm.y4a_dwc_amz_cop_ovr
         WHERE y4a_dwc_amz_cop_ovr.funding_type::text = 'Vendor Funded Sales Discount'::text AND y4a_dwc_amz_cop_ovr.invoice_date::date < date_trunc('month'::text, (now() + '1 mon'::interval)::date::timestamp with time zone)::date AND y4a_dwc_amz_cop_ovr.invoice_date::date >= date_trunc('month'::text, (now() - '2 mons'::interval)::date::timestamp with time zone)::date AND y4a_dwc_amz_cop_ovr.country::text = 'JPN'::text
         GROUP BY (to_char(y4a_dwc_amz_cop_ovr.invoice_date, 'YYYY-MM'::text)), y4a_dwc_amz_cop_ovr.country
       )
SELECT a.year_month,
   a.se_type,
   a.region_code,
   a.erp_amount,
   b.country::character(45) AS country,
   b.raw_amount,
   a.erp_amount - b.raw_amount AS recon
  FROM erp a
    FULL JOIN raw b ON
       CASE
           WHEN split_part(a.region_code::text, '-'::text, 2) = 'MXN'::text THEN 'MEX'::text
           WHEN split_part(a.region_code::text, '-'::text, 2) = 'UAE'::text THEN 'ARE'::text
           ELSE split_part(a.region_code::text, '-'::text, 2)
       END = split_part(b.country::text, '-'::text, 1) AND a.year_month = b.year_month;

