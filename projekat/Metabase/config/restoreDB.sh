#!/bin/bash

dropdb -h 127.0.0.1 -U $POSTGRES_USER -w -f metabasedb

pg_restore -h 127.0.0.1 -U $POSTGRES_USER -w -F c -C -d postgres /config/backup/metabasedb-yt-tt-analysis.dump