#!/bin/bash
# ForgeNX Engine UI release script
# Usage: ./release-ui.sh <version>
set -e

VERSION=${1:?"Usage: $0 <version>"}
IMAGE="ghcr.io/forgenx/forgenx-engine"

echo "=== ForgeNX Engine UI Release v${VERSION} ==="

# 1. Sync base EngineUI.jsx from forgenx-ui (preserves standalone additions)
echo "--- Note: EngineUI.jsx in ui/src/ is the standalone version with Logs+Info tabs ---"
echo "--- To update from forgenx-ui, manually merge changes into ui/src/EngineUI.jsx ---"

# 2. Build UI into static/
echo "--- Building UI ---"
cd ~/forgenx-engine/ui && npm run build

# 3. Build and push Docker image
echo "--- Building Docker image ---"
cd ~/forgenx-engine
docker build --build-arg VERSION=${VERSION} --build-arg BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ") -t ${IMAGE}:${VERSION} .
docker push ${IMAGE}:${VERSION}

# 4. Bump versions
echo "--- Bumping versions ---"
CURRENT=$(grep "^version:" ~/ForgeNX-store/forgenx-engine/umbrel-app.yml | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+')
echo "Bumping ${CURRENT} → ${VERSION}"

# Update store manifest and compose
sed -i "s/version: \"${CURRENT}\"/version: \"${VERSION}\"/" ~/ForgeNX-store/forgenx-engine/umbrel-app.yml
sed -i "s|${IMAGE}:[^ ]*|${IMAGE}:${VERSION}|g" ~/ForgeNX-store/forgenx-engine/docker-compose.yml

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

# Regenerate proxy conf with API routes
echo "--- Regenerating proxy conf ---"
python3 -c "
import sys; sys.path.insert(0, '/home/ellevix/forgenx-core')
from services.proxy_manager import register_app
register_app('forgenx-engine')
" 2>/dev/null || true
sudo docker exec forgenx-proxy nginx -s reload 2>/dev/null || true

echo ""
echo "=== Done! Sync App Store and update engine ==="
