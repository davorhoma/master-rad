#!/bin/bash

set -e

echo "#### Creating backup of the Superset database..."

docker exec superset-postgres \
	pg_dump \
	-U superset \
	-d superset \
	-F c \
	-f /tmp/supersetdb-yt-tt-analysis.dump

docker cp \
	superset-postgres:/tmp/supersetdb-yt-tt-analysis.dump \
	./config/backup/supersetdb-yt-tt-analysis.dump

docker exec superset-postgres \
	rm -f /tmp/supersetdb-yt-tt-analysis.dump

echo "#### Backup creation completed successfully!"
echo "#### Backup saved to: ./config/backup/supersetdb-yt-tt-analysis.dump"
