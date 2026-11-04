package repository

const (
	insertEventSQL = `INSERT INTO events
	(event_id, producer_id, receiver_id, event_type, schema_version, 
	tenant_id, occured_at, payload, raw_payload, status, validation_errors, 
	routing_target, source_topic, source_partition, source_offset)
	VALUES
	(NULLIF($1, ''),NULLIF($2, ''),NULLIF($3, ''),NULLIF($4, ''),NULLIF($5, ''), NULLIF($6, ''),
	$7,$8,$9,$10,$11,$12,$13,$14,$15) ON CONFLICT DO NOTHING`

	selectRAWByOffsetSQL = `SELECT raw_payload FROM events WHERE source_topic=$1 AND source_partition=$2 AND source_offset=$3`

	selectRawByIdentitySQL = `SELECT raw_payload FROM events WHERE tenant_id=$1 AND event_id=$2`

	insertQuarantineSQL = `INSERT INTO event_quarantine(source_topic, source_partition, source_offset, tenant_id, event_id, raw_payload, reason)
	VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(source_topic, source_partition, source_offset) DO NOTHING`
)
