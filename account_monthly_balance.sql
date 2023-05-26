WITH

raw_data AS (
-- Gets raw data from both pix and non-pix transactions

    SELECT *, "transfer_in" AS in_or_out, "non_pix" AS type
    FROM transfer_ins

    UNION ALL

    SELECT *, "transfer_out" AS in_or_out, "non_pix" AS type
    FROM transfer_outs

    UNION ALL

    SELECT *, "pix" AS type
    FROM pix_movements
    
),

report AS (
-- Gets 2020's Account Monthly Balance

    SELECT
        month(from_unixtime(transaction_requested_at))                  AS Month,
        account_id                                                      AS Customer,
        SUM(CASE WHEN in_or_out LIKE '%in%' THEN amount ELSE 0 END)     AS TotalTransferIn,
        SUM(CASE WHEN in_or_out LIKE '%out%' THEN amount ELSE 0 END)    AS TotalTransferOut,
        (SUM(CASE WHEN in_or_out LIKE '%in%' THEN amount ELSE 0 END)
        -SUM(CASE WHEN in_or_out LIKE '%out%' THEN amount ELSE 0 END))  AS AccountMonthlyBalance    

    FROM raw_data
   WHERE from_unixtime(transaction_requested_at) >= "2020-01-01"
     AND from_unixtime(transaction_requested_at) <  "2021-01-01"
     AND status = "completed"
    GROUP BY 1,2
    
)

SELECT *
FROM report
