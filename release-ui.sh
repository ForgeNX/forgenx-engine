#!/bin/bash
# ForgeNX Engine UI release script
# Usage: ./release-ui.sh <version>
# Example: ./release-ui.sh 1.0.86

set -e

VERSION=${1:?"Usage: $0 <version>"}

echo "=== ForgeNX Engine UI Release v${VERSION} ==="

# 1. Copy latest EngineUI.jsx from forgenx-ui
echo "--- Copying EngineUI.jsx from forgenx-ui ---"
cp ~/forgenx-ui/src/pages/EngineUI.jsx ~/forgenx-engine/ui/src/EngineUI.jsx

# 2. Build the Vite app into static/
echo "--- Building UI ---"
cd ~/forgenx-engine/ui && npm run build

# 3. Build and push new engine Docker image
echo "--- Building Docker image ---"
cd ~/forgenx-engine
docker build -t ghcr.io/forgenx/forgenx-engine:${VERSION} .
docker push ghcr.io/forgenx/forgenx-engine:${VERSION}

# 4. Bump versions in store
echo "--- Updating store ---"
CURRENT=$(grep "^version:" ~/ForgeNX-store/forgenx-engine/umbrel-app.yml | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+')
sed -i "s/version: \"${CURRENT}\"/version: \"${VERSION}\"/" ~/ForgeNX-store/forgenx-engine/umbrel-app.yml
sed -i "s|ghcr.io/forgenx/forgenx-engine:${CURRENT}|ghcr.io/forgenx/forgenx-engine:${VERSION}|" ~/ForgeNX-store/forgenx-engine/docker-compose.yml

# 5. Commit engine repo
echo "--- Committing engine repo ---"
cd ~/forgenx-engine
git add ui/src/EngineUI.jsx static/ umbrel-app.yml
git commit -m "engine: release v${VERSION} UI update"
git push

# 6. Commit store
echo "--- Committing store ---"
cd ~/ForgeNX-store
git add forgenx-engine/umbrel-app.yml forgenx-engine/docker-compose.yml
git commit -m "forgenx-engine: bump to v${VERSION}"
git push

echo ""
echo "=== Done! Sync App Store and update engine ==="
