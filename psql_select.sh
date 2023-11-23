psql -U yzpt carburants <<EOF
SELECT id, record_timestamp, ville, adresse, latitude, longitude 
FROM records 
WHERE lower(ville) = 'lille'
AND record_timestamp = (SELECT MAX(record_timestamp) FROM records WHERE lower(ville) = 'lille')
ORDER BY record_timestamp DESC;
\q
EOF