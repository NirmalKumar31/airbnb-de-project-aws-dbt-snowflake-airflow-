-- tests/assert_silver_hosts_no_duplicates.sql
SELECT host_id, COUNT(*) AS cnt
FROM {{ ref('silver_hosts') }}
GROUP BY host_id
HAVING cnt > 1