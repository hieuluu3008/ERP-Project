-- DROP PROCEDURE y4a_erp.sp_y4a_erp_selling_expense_acr_est_log();
CREATE OR REPLACE PROCEDURE y4a_erp.sp_y4a_erp_selling_expense_acr_est_log()
LANGUAGE plpgsql
AS $procedure$
	BEGIN
		insert into y4a_erp.tb_y4a_erp_selling_expense_acr_est_log
		(
WITH cte AS (
SELECT 'GENERAL'::text AS journal_template_name,
           'IM_SELEXP'::text AS journal_batch_name,
           NULL::text AS line_no,
           'G/L Account'::text AS account_type,
           NULL::text AS account_no,
           posting_date,
           NULL::text AS document_type,
           NULL::text AS document_no,
           concat(internal_sales_channel,'-',right(country,3),': Accrual estimate ',note,' exps Inv# ',invoice_number,'_',to_char(document_date,'yymmdd')) as description,
           NULL::text AS bal_account_no,
           currency AS currency_code,
           'G/L Account'::text AS bal_account_type,
           document_date,
           invoice_number AS external_document_no,
           sales_channel AS channel_code,
           internal_sales_channel,
           platform AS platform_code,
           country AS region_code,
           rate,
           original_invoice,
           company,
           original_y4a_company_id,
           po_number,
           is_processed,
           note,
           accrual_amt AS amount,
           amount AS si_amount,
			"type"
          FROM y4a_erp.tb_y4a_erp_selling_expense_acr_est_dtl
         )
SELECT cte.journal_template_name,
   cte.journal_batch_name,
   cte.line_no,
   cte.account_type,
   cte.account_no,
   cte.posting_date,
   cte.document_type,
   cte.document_no,
   cte.description,
   cte.bal_account_no,
   cte.currency_code,
   cte.amount,
   cte.bal_account_type,
   cte.document_date,
   cte.external_document_no,
   cte.channel_code,
   cte.platform_code,
   cte.region_code,
   cte.internal_sales_channel,
   cte.si_amount,
   cte.rate,
   company,
   original_y4a_company_id,
   original_invoice,
   po_number,
   is_processed,
   now() AS run_time,
   note,
	"type"
  FROM cte
 ORDER BY cte.document_date, cte.external_document_no, cte.rate
);
	END;
$procedure$
;

