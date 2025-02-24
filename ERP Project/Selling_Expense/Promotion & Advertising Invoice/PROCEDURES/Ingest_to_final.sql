INSERT INTO y4a_erp.y4a_erp_prod_selling_expense_incremental
(
SELECT a.* FROM  y4a_erp.y4a_erp_prod_selling_expense_incremental_daily AS a
LEFT JOIN y4a_erp.y4a_erp_prod_selling_expense_incremental AS  b
ON  a.external_document_no = b.external_document_no AND a.region_code = b.region_code
WHERE 1=1
AND b IS NULL
AND a.amount !=0
AND a.posting_date::date <= '2025-01-25' -- cut-off time hàng tháng
)
;
update y4a_erp.y4a_erp_prod_selling_expense_incremental set is_processed = 0
where is_processed = 2
;
