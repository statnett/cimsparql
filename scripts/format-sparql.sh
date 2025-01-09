#!/bin/bash

file="$1"

set -e

trap 'rm -f "$file.tmp"' EXIT

sed -i 's/\${/__DOLLAR____LEFTBRACE__/g; s/__DOLLAR____LEFTBRACE__\([^}]*\)}/__DOLLAR____LEFTBRACE__\1__RIGHTBRACE__/g' "$file"
npx sparql-formatter "$file" > "$file.tmp" && mv "$file.tmp" "$file"
sed -i 's/__DOLLAR____LEFTBRACE__/\${/g; s/__RIGHTBRACE__/}/g' "$file"
