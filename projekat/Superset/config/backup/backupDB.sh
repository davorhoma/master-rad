#!/bin/bash
echo "#### Creating backup of the Superset database..."
pg_dump -h 127.0.0.1 -d superset -U superset -w -f /config/backup/supersetdb-yt-tt-analysis.dump -F c
if [ $? -eq 0 ]; then
    echo "#### Backup creation completed successfully!"
else
    echo "#### Backup creation failed!"
fi