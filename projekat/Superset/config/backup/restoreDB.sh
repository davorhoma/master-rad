#!/bin/bash

dropdb -h 127.0.0.1 -U superset -w superset

pg_restore -h 127.0.0.1 -U superset -w -F c -C -d postgres /config/backup/supersetdb-yt-tt-analysis.dump