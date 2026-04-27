#!/bin/bash
# Generates .htpasswd with bcrypt-hashed password for nginx basic auth
# Requires: docker

set -euo pipefail

HTFILE="$(dirname "$0")/nginx/.htpasswd"

if [ -n "${1:-}" ] && [ -n "${2:-}" ]; then
    USERNAME="$1"
    PASSWORD="$2"
else
    read -rp "Username: " USERNAME
    read -rsp "Password: " PASSWORD
    echo
fi

docker run --rm httpd:alpine htpasswd -Bbn "$USERNAME" "$PASSWORD" > "$HTFILE"
echo "Credentials saved to $HTFILE"
