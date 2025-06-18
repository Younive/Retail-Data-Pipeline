SELECT
  c.country,
  c.alpha3Code,
  COUNT(fi.invoice_id) AS total_invoices,
  SUM(fi.total) AS total_revenue
FROM {{ ref('fct_invoices') }} fi
JOIN {{ ref('dim_customer') }} c ON fi.customer_id = c.customer_id
GROUP BY c.country, c.alpha3Code
ORDER BY total_revenue DESC
LIMIT 10