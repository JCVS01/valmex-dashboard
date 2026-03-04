#!/bin/bash
# Auto-sync: pull changes, stage, commit, and push
# Usage: bash sync.sh "commit message"
#   or just: bash sync.sh  (uses default message)

cd "$(dirname "$0")"

MSG="${1:-Auto-sync $(date '+%Y-%m-%d %H:%M')}"

echo "=== Pulling latest changes..."
git pull --rebase origin main

echo "=== Staging changes..."
git add -A

# Check if there are changes to commit
if git diff --cached --quiet; then
    echo "=== No changes to commit."
else
    echo "=== Committing: $MSG"
    git commit -m "$MSG"
    echo "=== Pushing to origin/main..."
    git push origin main
    echo "=== Done!"
fi
