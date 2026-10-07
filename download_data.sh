#!/usr/bin/env bash
set -euo pipefail

URL="https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2026-10-01.zip"
DEST="./data"
ZIP="$(mktemp --suffix=.zip)"

trap 'rm -f "$ZIP"' EXIT

mkdir -p "$DEST"

echo "Downloading..."
curl --fail --location --retry 3 --progress-bar -o "$ZIP" "$URL"

echo "Unzipping to $DEST..."
unzip -o "$ZIP" -d "$DEST"

echo "Done."
ls -lh "$DEST"