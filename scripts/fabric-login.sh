#!/usr/bin/env sh
# Signs fab and az in as the service principal from .env (client_id, ternan_id, sp_secret).
# Reads only those three keys (never sources .env) and prints no secrets.
set -eu
cd "$(dirname "$0")/.."

get() { grep -E "^$1=" .env | head -n 1 | cut -d= -f2- | tr -d '\r' | sed -e 's/^"//' -e 's/"$//'; }

client_id=$(get client_id)
tenant_id=$(get ternan_id)
secret=$(get sp_secret)

fab auth login -u "$client_id" -p "$secret" -t "$tenant_id" > /dev/null
az login --service-principal -u "$client_id" --password="$secret" -t "$tenant_id" --allow-no-subscriptions > /dev/null
echo "fab and az signed in as service principal $client_id"
