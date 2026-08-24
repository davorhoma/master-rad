#!/bin/bash
echo "#### Creating backup of the Metabase database..."
pg_dump -h 127.0.0.1 -d metabasedb -U $POSTGRES_USER -w -f /config/backup/metabasedb-yt-tt-analysis.dump -F c
if [ $? -eq 0 ]; then
    echo "#### Backup creation completed successfully!"
else
    echo "#### Backup creation failed!"
fi