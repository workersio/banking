#!/bin/sh
set -eu
pkg install -y python311 py311-sqlite3
ln -sf python3.11 /usr/local/bin/python3
