package engine

// Coin configs left behind by an uninstalled coin app.
//
// Coin configs live in the shared coins folder, outside each coin app's own
// data, so uninstalling a coin app leaves its config there. Nothing in the app
// runs at uninstall to remove it, so the engine checks at start-up: a config
// whose owner_app is no longer installed is moved into coins/removed/, with
// its SV2 keys. It is never deleted: a coin app reinstalled with its data kept
// brings its settings back from there.
//
// "Still installed" — either is enough:
//   - ForgeNX: the app's folder in /opt/forgenx/apps still has its
//     docker-compose.yml. (ForgeNX's Stop removes an app's containers, and
//     "Uninstall, keep data" leaves the folder holding only data/, so neither
//     the containers alone nor the folder alone would do.)
//   - Any OS, umbrelOS included: Docker still has a container for the app —
//     its node container (<app>-node) or any container in its compose project.
//
// When Docker can't be asked, or answers anything unexpected, the engine can't
// tell, and the config stays where it is. It only moves a config when the
// folder check and Docker both say the app is gone, every time they're asked
// over about 20 seconds.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type ownerState int

const (
	ownerInstalled ownerState = iota
	ownerGone
	ownerUnknown
)

func (s ownerState) String() string {
	switch s {
	case ownerInstalled:
		return "installed"
	case ownerGone:
		return "not installed"
	}
	return "can't tell"
}

// Overridable for tests.
var (
	ownerAppsDir      = "/opt/forgenx/apps"
	ownerDockerSocket = "/var/run/docker.sock"
	ownerRetries      = 40
	ownerRetryWait    = 500 * time.Millisecond
)

// RemovedDirName is the folder inside the coins folder that left-over configs
// are moved into.
const RemovedDirName = "removed"

var ownerAppName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// ownerAppState reports whether the coin app named app is still installed.
func ownerAppState(app string) ownerState {
	if !ownerAppName.MatchString(app) {
		return ownerUnknown
	}
	if _, err := os.Stat(filepath.Join(ownerAppsDir, app, "docker-compose.yml")); err == nil {
		return ownerInstalled
	}
	return dockerHasApp(app)
}

func dockerHasApp(app string) ownerState {
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "unix", ownerDockerSocket)
			},
		},
	}

	// The node container, by name.
	resp, err := client.Get("http://docker/containers/" + url.PathEscape(app+"-node") + "/json")
	if err != nil {
		return ownerUnknown
	}
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return ownerInstalled
	case http.StatusNotFound:
	default:
		return ownerUnknown
	}

	// Any container in the app's compose project, stopped ones included.
	filters := fmt.Sprintf(`{"label":["com.docker.compose.project=%s"]}`, app)
	resp, err = client.Get("http://docker/containers/json?all=1&filters=" + url.QueryEscape(filters))
	if err != nil {
		return ownerUnknown
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ownerUnknown
	}
	var list []json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return ownerUnknown
	}
	if len(list) > 0 {
		return ownerInstalled
	}
	return ownerGone
}

// settleOwnerApp decides what to do with the coin config dir/<symbol>.json,
// written by the coin app named app. It returns true when the config should be
// loaded as usual, false when it was moved aside (or should be left alone
// without loading, because moving it failed).
func settleOwnerApp(dir, symbol, app string, info, warn func(string, ...interface{})) bool {
	state := ownerAppState(app)
	// Only "not installed" is checked again: the engine can start before the
	// apps folder or Docker has everything in place after a reboot.
	for i := 0; i < ownerRetries && state == ownerGone; i++ {
		time.Sleep(ownerRetryWait)
		state = ownerAppState(app)
	}

	switch state {
	case ownerInstalled:
		info("[%s] owner app %s is installed", symbol, app)
		return true
	case ownerUnknown:
		info("[%s] can't tell whether owner app %s is installed — keeping its config", symbol, app)
		return true
	}

	moved, err := moveAside(dir, symbol)
	if err != nil {
		warn("[%s] owner app %s is not installed, but its config couldn't be moved aside: %v", symbol, app, err)
		return false
	}
	warn("[%s] owner app %s is not installed — moved %s into %s/ (kept for a reinstall)", symbol, app, strings.Join(moved, ", "), RemovedDirName)
	return false
}

// moveAside moves a coin's config, then its SV2 keys, into dir/removed/,
// replacing any older copies there. Returns the files it moved.
func moveAside(dir, symbol string) ([]string, error) {
	removed := filepath.Join(dir, RemovedDirName)
	if err := os.MkdirAll(removed, 0755); err != nil {
		return nil, err
	}
	sym := strings.ToLower(symbol)
	var moved []string
	for _, name := range []string{sym + ".json", sym + "_sv2.key", sym + "_sv2_authority.key"} {
		src := filepath.Join(dir, name)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		if err := os.Rename(src, filepath.Join(removed, name)); err != nil {
			if len(moved) == 0 {
				return nil, err
			}
			// The config is already out of the way; a key that won't move just
			// stays where it is.
			continue
		}
		moved = append(moved, name)
	}
	if len(moved) == 0 {
		return nil, fmt.Errorf("%s.json not found", sym)
	}
	return moved, nil
}
