#!/usr/bin/env bash
# deploy.sh - build, publish and list a new ForgeNX Engine version in the store.
#
#   ./deploy.sh 1.0.369
#
# Stops at the first thing that fails. When it finishes, update the engine from
# the App Store as usual - the installed version is left alone on purpose, so the
# store sees the new version as an update to offer.
set -euo pipefail

VERSION="${1:-}"
IMAGE="ghcr.io/forgenx/forgenx-engine"
ENGINE="$HOME/forgenx-engine"
STORE_REPO="$HOME/ForgeNX-store"
STORE="$STORE_REPO/forgenx-engine"
export PATH="$PATH:/usr/local/go/bin"

step() { printf '\n=== %s ===\n' "$1"; }
fail() { printf '\nSTOPPED: %s\n' "$1" >&2; exit 1; }

[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "give a version, e.g. ./deploy.sh 1.0.369"

CURRENT=$(grep -oE '^version: "[0-9.]+"' "$STORE/umbrel-app.yml" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
[[ "$CURRENT" != "$VERSION" ]] || fail "the store is already at $VERSION - pick the next number"
echo "Store is at $CURRENT, deploying $VERSION"

cd "$ENGINE"

step "Checking the engine repo"
# The image is built from the working tree, so anything uncommitted would ship
# without being in git.
if [[ -n "$(git status --porcelain -- pkg cmd go.mod go.sum Dockerfile static)" ]]; then
  git status --short -- pkg cmd go.mod go.sum Dockerfile static
  fail "uncommitted engine changes - commit them first"
fi
git fetch -q origin
if [[ -n "$(git log origin/main..HEAD --oneline)" ]]; then
  echo "Pushing local commits:"
  git log origin/main..HEAD --oneline
  git push -q origin main
fi

step "Compiling"
go build ./... || fail "go build failed"
echo "engine builds"

step "Building image $IMAGE:$VERSION"
if docker manifest inspect "$IMAGE:$VERSION" >/dev/null 2>&1; then
  fail "$IMAGE:$VERSION is already on GHCR - pick the next number"
fi
docker build -q \
  --build-arg VERSION="$VERSION" \
  --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -t "$IMAGE:$VERSION" .

step "Pushing to GHCR"
docker push -q "$IMAGE:$VERSION"

step "Listing $VERSION in the store"
sed -i -E "s|^version: \"[0-9]+\.[0-9]+\.[0-9]+\"|version: \"$VERSION\"|" "$STORE/umbrel-app.yml"
sed -i -E "s|$IMAGE:[0-9]+\.[0-9]+\.[0-9]+|$IMAGE:$VERSION|" "$STORE/docker-compose.yml"
grep -q "version: \"$VERSION\"" "$STORE/umbrel-app.yml" || fail "store manifest did not take the new version"
grep -q "$IMAGE:$VERSION" "$STORE/docker-compose.yml" || fail "store compose did not take the new image"
cd "$STORE_REPO"
git add forgenx-engine
git commit -q -m "forgenx-engine: bump to v$VERSION"
git push -q origin main

step "Refreshing the store cache"
if curl -s -X POST "http://localhost:8001/api/forgenx/refresh?force=true" >/dev/null 2>&1; then
  echo "store cache refreshed"
else
  echo "store refresh failed (non-fatal) - use Sync in the App Store"
fi

printf '\nDone. %s is published and listed.\nNow update the engine from the App Store.\n' "$VERSION"
