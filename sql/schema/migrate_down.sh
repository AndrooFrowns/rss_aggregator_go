#!/usr/bin/bash
# provide the connection string to migrate up
goose postgres "$1" down

