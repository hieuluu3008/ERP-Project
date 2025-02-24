-- DROP PROCEDURE y4a_erp.sp_y4a_erp_sel_exp_cop_inv_api_upt();
CREATE OR REPLACE PROCEDURE y4a_erp.sp_y4a_erp_sel_exp_cop_inv_api_upt()
LANGUAGE plpgsql
AS $procedure$
	BEGIN
TRUNCATE TABLE y4a_erp.tb_y4a_erp_sel_exp_cop_inv_update ;
	INSERT INTO  	y4a_erp.tb_y4a_erp_sel_exp_cop_inv_update
		(
WITH cop_inv_ovr AS ( -- Lấy thông tin về invoice amount của Promotion
SELECT
	DISTINCT t1.invoice_id  AS invoice_number,
	t1.invoice_date,
	t1.funding_type,
       CASE
           WHEN upper(t1.funding_type) ~~ '%STRAIGHT PAYMENT%' THEN
           				CASE
	                			WHEN upper(t1.agreement_title) ~~ '%MERCHANDISING%' THEN 'Base merchandising fee'
								WHEN upper(t1.agreement_title) ~~ '%BORN TO RUN%' THEN 'Born to run'
								WHEN upper(t1.agreement_title) ~~ '%VINE%' AND upper(t1.agreement_title) !~~ '%STRAIGHT_PAYMENT%' THEN 'VINE_new'
								WHEN upper(t1.agreement_title) ~~ '%VINE%' AND upper(t1.agreement_title) ~~ '%STRAIGHT_PAYMENT%' THEN 'VINE_old'
								WHEN t2.transaction_type = 'AVS - SVS' THEN 'AVS_SVS'
								ELSE 'Other deduction'
							END
			WHEN upper(t1.funding_type) ~~ '%VENDOR%' THEN 'Vendor Funded Sales Discount'
			WHEN upper(t1.funding_type) ~~ '%PRICE PROTECTION%' THEN 'Price Protection'
			WHEN upper(t1.funding_type) ~~ '%GUARANTEED%' THEN 'Guaranteed Minimum Margin'
			WHEN upper(t1.funding_type) ~~ '%DAMAGE%' THEN 'Damage Allowance'
			WHEN upper(t1.funding_type) ~~ '%ACCRUAL%' THEN 'Accrual'
		ELSE NULL
	END AS sub_funding_type,
	t1.agreement_id,
	t1.agreement_title,
	CASE
		WHEN t1.currency = 'USA' THEN 'USD'
		WHEN t1.currency IS NULL
		AND t1.country = 'USA' THEN 'USD'
		ELSE t1.currency
	END AS currency,
	t1.country,
	t1.original_balance AS cop_ovr_amt
FROM
	y4a_cdm.y4a_dwc_amz_cop_ovr t1
LEFT JOIN y4a_cdm.y4a_dwc_amz_cop_str t2 -- bổ trợ data transaction_type cho bảng coop invoice
               ON
	t1.invoice_id = t2.invoice_number
               )
               ,
total_quantity AS (
               SELECT invoice_number, sum(quantity) AS total_quantity
               ,sum(promotion_cost) AS total_promotion_cost
               FROM y4a_cdm.y4a_dwc_amz_cop_pro
			    GROUP BY 1
               )
               ,
clp_fee AS -- data ngưng update từ 2023
               (
			SELECT
				invoice_number,
			    sum(total_clip_fee) AS total_clip_fee
			   FROM
				y4a_cdm.y4a_dwc_amz_cop_clp
			GROUP BY
				invoice_number
)
		SELECT
		  	to_char(coalesce(t1.order_date, t1.ship_date)::date, 'YYYY') as year_order_date,
		  	coalesce (t1.order_date, t1.ship_date)::date as report_date,		       
			t1.invoice_number,
			t1.invoice_date,
			t2.funding_type,
			t2.sub_funding_type,
			t2.currency,
			t1.country,
					CASE
				WHEN COALESCE(t3.promotion_id , t1.promotion_id) IS NULL THEN 'VM'
				ELSE 'Portal'
			END AS booked_type,
			COALESCE(t3.promotion_id , t1.promotion_id) AS promotion_id ,
			asin,
			t1.quantity,
			t5.total_quantity,
			t2.cop_ovr_amt,
			COALESCE(t4.total_clip_fee,(t2.cop_ovr_amt - t5.total_promotion_cost)) AS clp_fee,
			t1.promotion_cost AS promotion_cost,
			COALESCE(round(t1.promotion_cost + (t4.total_clip_fee/t5.total_quantity)*t1.quantity,2),
					round(t1.promotion_cost + ((t2.cop_ovr_amt - t5.total_promotion_cost)/t5.total_quantity) *t1.quantity ,2))
					AS cop_fee_by_sku ---- case for AUS with NO DATA OF clp fee
		FROM
			y4a_cdm.y4a_dwc_amz_cop_pro AS t1 -- bảng data detail từng promotion chạy cho ASIN tương ứng
		LEFT JOIN cop_inv_ovr AS t2 ON
			t1.invoice_number = t2.invoice_number
		LEFT JOIN (select distinct promotion_id, funding_agreement from y4a_cdm.y4a_dwc_amz_avc_pro_inf) t3 on -- Lấy promotion_id
			t3.funding_agreement = t2.agreement_id
		LEFT JOIN clp_fee t4 ON
			t1.invoice_number = t4.invoice_number
		LEFT JOIN total_quantity AS t5 ON
			t1.invoice_number = t5.invoice_number
		WHERE t1.invoice_date >='2024-06-01'
	
UNION ALL
-- những invoice_number mà bảng cop_pro không có
		SELECT
			to_char(invoice_date,'YYYY') AS year_order_date,
			invoice_date::date AS report_date,
			invoice_id,
			invoice_date::date,
			'STRAIGHT_PAYMENT' AS funding_type,
			'Base merchandising fee' sub_funding_type,
			currency,
			country,
			'Base merchandising fee' AS booked_type,
			split_part(agreement_title,':',2)  AS promotion_id,
			NULL AS asin,
			NULL AS quantity,
			NULL AS total_quantity,
			round(original_balance,2) AS cop_ovr_amt,
			NULL AS clp_fee,
			NULL AS promotion_cost,
			round(original_balance,2) AS cop_fee_by_sku
		FROM
			y4a_cdm.y4a_dwc_amz_cop_ovr
		WHERE upper(funding_type) ~~ '%STRAIGHT PAYMENT%' AND upper(agreement_title) ~~ '%MERCHANDISING%'
		AND invoice_date >='2024-06-01'
	) ;
	END;
$procedure$
;

