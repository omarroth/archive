#!/bin/bash

createdb archive
#createuser kemal
psql -c "CREATE USER kemal WITH PASSWORD 'kemal';"
psql archive < config/sql/channels.sql
psql archive < config/sql/videos.sql
