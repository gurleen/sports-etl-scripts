#!/bin/bash

# Parse DATABASE_URL and export individual PostgreSQL environment variables
# Usage: source ./scripts/parse_database_url.sh
# Or add to your shell initialization or before running dbt commands

if [ -z "$DATABASE_URL" ]; then
    echo "Error: DATABASE_URL environment variable is not set"
    exit 1
fi

# Remove the scheme (postgresql://, postgres://, etc.)
DB_URL="${DATABASE_URL#*://}"

# Extract credentials and host
if [[ "$DB_URL" == *"@"* ]]; then
    CREDENTIALS="${DB_URL%%@*}"
    HOST_AND_DB="${DB_URL##*@}"
    
    # Extract username and password
    if [[ "$CREDENTIALS" == *":"* ]]; then
        POSTGRES_USER="${CREDENTIALS%%:*}"
        POSTGRES_PASSWORD="${CREDENTIALS#*:}"
    else
        POSTGRES_USER="$CREDENTIALS"
        POSTGRES_PASSWORD=""
    fi
else
    HOST_AND_DB="$DB_URL"
    POSTGRES_USER="postgres"
    POSTGRES_PASSWORD=""
fi

# Extract host, port, and database
if [[ "$HOST_AND_DB" == *"/"* ]]; then
    HOST_PORT="${HOST_AND_DB%%/*}"
    POSTGRES_DB="${HOST_AND_DB##*/}"
else
    HOST_PORT="$HOST_AND_DB"
    POSTGRES_DB="postgres"
fi

# Extract host and port
if [[ "$HOST_PORT" == *":"* ]]; then
    POSTGRES_HOST="${HOST_PORT%%:*}"
    POSTGRES_PORT="${HOST_PORT##*:}"
else
    POSTGRES_HOST="$HOST_PORT"
    POSTGRES_PORT="5432"
fi

# Export the variables
export POSTGRES_HOST
export POSTGRES_PORT
export POSTGRES_USER
export POSTGRES_PASSWORD
export POSTGRES_DB

# Verify
echo "DATABASE_URL parsed successfully:"
echo "export POSTGRES_HOST=$POSTGRES_HOST"
echo "export POSTGRES_PORT=$POSTGRES_PORT"
echo "export POSTGRES_USER=$POSTGRES_USER"
echo "export POSTGRES_DB=$POSTGRES_DB"
echo "export POSTGRES_PASSWORD=$POSTGRES_PASSWORD"