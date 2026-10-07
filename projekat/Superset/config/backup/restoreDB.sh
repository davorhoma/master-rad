#!/bin/bash

set -e

BACKUP_FILE="/config/backup/supersetdb-yt-tt-analysis.dump"

echo "#### Restoring the Superset database..."

docker cp \
	./config/backup/supersetdb-yt-tt-analysis.dump \
	superset-postgres:/tmp/supersetdb-yt-tt-analysis.dump

docker exec superset-postgres \
	dropdb \
	-U superset \
	--if-exists \
	superset

docker exec superset-postgres \
	pg_restore \
	-U superset \
	-F c \
	-C \
	-d postgres \
	/tmp/supersetdb-yt-tt-analysis.dump

docker exec superset-postgres \
	rm -f /tmp/supersetdb-yt-tt-analysis.dump

echo "#### Database restore completed successfully!"
