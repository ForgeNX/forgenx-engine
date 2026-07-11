#!/bin/bash
# ForgeNX Engine UI release script
# Usage: ./release-ui.sh <version>
set -e

VERSION=${1:?"Usage: $0 <version>"}
IMAGE="ghcr.io/forgenx/forgenx-engine"

echo "=== ForgeNX Engine UI Release v${VERSION} ==="

# 1. Copy latest EngineUI.jsx from forgenx-ui
echo "--- Copying EngineUI.jsx ---"
cp ~/forgenx-ui/src/pages/EngineUI.jsx ~/forgenx-engine/ui/src/EngineUI.jsx

# 2. Build UI into static/
echo "--- Building UI ---"
cd ~/forgenx-engine/ui && npm run build

# 3. Build and push Docker image
echo "--- Building Docker image ---"
cd ~/forgenx-engine
docker build -t ${IMAGE}:${VERSION} .
docker push ${IMAGE}:${VERSION}

# 4. Bump versions
echo "--- Bumping versions ---"
CURRENT=$(grep "^version:" ~/ForgeNX-store/forgenx-engine/umbrel-app.yml | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+')
echo "Bumping ${CURRENT} → ${VERSION}"

# Update store manifest and compose
sed -i "s/version: \"${CURRENT}\"/version: \"${VERSION}\"/" ~/ForgeNX-store/forgenx-engine/umbrel-app.yml
sed -i "s|${IMAGE}:${CURRENT}|${IMAGE}:${VERSION}|" ~/ForgeNX-store/forgenx-engine/docker-compose.yml

# Update engine repo manifest
sed -i "s/version: \"${CURRENT}\"/version: \"${VERSION}\"/" ~/forgenx-engine/umbrel-app.yml

# 5. Commit engine repo
echo "--- Committing engine repo ---"
cd ~/forgenx-engine
git add ui/src/EngineUI.jsx static/ umbrel-app.yml pkg/coinapi/handlers.go
git commit -m "engine: release v${VERSION}" || echo "Nothing new in engine repo"
git push || true

# 6. Commit store
echo "--- Committing store ---"
cd ~/ForgeNX-store
git add forgenx-engine/umbrel-app.yml forgenx-engine/docker-compose.yml
git commit -m "forgenx-engine: bump to v${VERSION}"
git push

echo ""
echo "=== Done! Sync App Store and update engine ==="
