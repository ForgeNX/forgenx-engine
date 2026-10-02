package engine

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeDocker answers the two Docker API calls ownerapp.go makes, over a unix
// socket, like the real one.
type fakeDocker struct {
	nodes    map[string]bool // container names that exist
	projects map[string]int  // compose project -> number of containers
	status   int             // when set, every answer has this status
	calls    int
}

func (f *fakeDocker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.calls++
	if f.status != 0 {
		w.WriteHeader(f.status)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/containers/") && strings.HasSuffix(r.URL.Path, "/json") && r.URL.Path != "/containers/json" {
		name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/containers/"), "/json")
		if f.nodes[name] {
			w.Write([]byte(`{"Name":"/` + name + `"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if r.URL.Path == "/containers/json" {
		var filters struct {
			Label []string `json:"label"`
		}
		json.Unmarshal([]byte(r.URL.Query().Get("filters")), &filters)
		n := 0
		for _, l := range filters.Label {
			n += f.projects[strings.TrimPrefix(l, "com.docker.compose.project=")]
		}
		list := make([]map[string]string, n)
		for i := range list {
			list[i] = map[string]string{"Id": fmt.Sprint(i)}
		}
		json.NewEncoder(w).Encode(list)
		return
	}
	w.WriteHeader(http.StatusBadRequest)
}

type ownerFixture struct {
	coins, apps string
	docker      *fakeDocker
	logs        []string
}

func newOwnerFixture(t *testing.T, withDocker bool) *ownerFixture {
	t.Helper()
	base := t.TempDir()
	fx := &ownerFixture{coins: filepath.Join(base, "coins"), apps: filepath.Join(base, "apps")}
	os.MkdirAll(fx.coins, 0755)
	os.MkdirAll(fx.apps, 0755)

	old := []interface{}{ownerAppsDir, ownerDockerSocket, ownerRetries, ownerRetryWait}
	t.Cleanup(func() {
		ownerAppsDir, ownerDockerSocket = old[0].(string), old[1].(string)
		ownerRetries, ownerRetryWait = old[2].(int), old[3].(time.Duration)
	})
	ownerAppsDir = fx.apps
	ownerDockerSocket = filepath.Join(base, "docker.sock")
	ownerRetries, ownerRetryWait = 3, time.Millisecond

	if withDocker {
		fx.docker = &fakeDocker{nodes: map[string]bool{}, projects: map[string]int{}}
		ln, err := net.Listen("unix", ownerDockerSocket)
		if err != nil {
			t.Fatal(err)
		}
		srv := &http.Server{Handler: fx.docker}
		go srv.Serve(ln)
		t.Cleanup(func() { srv.Close() })
	}
	return fx
}

func (fx *ownerFixture) writeCoin(t *testing.T, sym string, keys bool) {
	t.Helper()
	os.WriteFile(filepath.Join(fx.coins, sym+".json"), []byte(`{"owner_app":"x"}`), 0644)
	if keys {
		os.WriteFile(filepath.Join(fx.coins, sym+"_sv2.key"), []byte("k1"), 0600)
		os.WriteFile(filepath.Join(fx.coins, sym+"_sv2_authority.key"), []byte("k2"), 0600)
	}
}

func (fx *ownerFixture) settle(sym, app string) bool {
	logf := func(f string, a ...interface{}) { fx.logs = append(fx.logs, fmt.Sprintf(f, a...)) }
	return settleOwnerApp(fx.coins, strings.ToUpper(sym), app, logf, logf)
}

func exists(p string) bool { _, err := os.Stat(p); return err == nil }

func TestOwnerInstalledOnForgeNX(t *testing.T) {
	// No Docker socket at all: the ForgeNX folder alone must be enough, and
	// a stopped app (no containers) still counts as installed.
	fx := newOwnerFixture(t, false)
	os.MkdirAll(filepath.Join(fx.apps, "forgebch", "data"), 0755)
	os.WriteFile(filepath.Join(fx.apps, "forgebch", "docker-compose.yml"), nil, 0644)
	fx.writeCoin(t, "bch", true)
	if !fx.settle("bch", "forgebch") {
		t.Fatal("installed app's config was not kept")
	}
	if !exists(filepath.Join(fx.coins, "bch.json")) {
		t.Fatal("config moved")
	}
}

func TestOwnerInstalledByNodeContainer(t *testing.T) {
	// umbrelOS: no ForgeNX folder, but the node container exists.
	fx := newOwnerFixture(t, true)
	fx.docker.nodes["forgebch-node"] = true
	fx.writeCoin(t, "bch", true)
	if !fx.settle("bch", "forgebch") || !exists(filepath.Join(fx.coins, "bch.json")) {
		t.Fatal("config of an app with a node container was not kept")
	}
}

func TestOwnerInstalledByComposeProject(t *testing.T) {
	// A container in the app's compose project, under another name.
	fx := newOwnerFixture(t, true)
	fx.docker.projects["forgedgb"] = 2
	fx.writeCoin(t, "dgb", false)
	if !fx.settle("dgb", "forgedgb") || !exists(filepath.Join(fx.coins, "dgb.json")) {
		t.Fatal("config of an app with project containers was not kept")
	}
}

func TestOwnerGoneMovesConfigAndKeys(t *testing.T) {
	fx := newOwnerFixture(t, true)
	fx.docker.projects["forgedgb"] = 1 // another app: must not count
	fx.writeCoin(t, "bch", true)
	fx.writeCoin(t, "dgb", false)
	if fx.settle("bch", "forgebch") {
		t.Fatal("uninstalled app's config was loaded")
	}
	for _, f := range []string{"bch.json", "bch_sv2.key", "bch_sv2_authority.key"} {
		if exists(filepath.Join(fx.coins, f)) {
			t.Errorf("%s still in the coins folder", f)
		}
		if !exists(filepath.Join(fx.coins, RemovedDirName, f)) {
			t.Errorf("%s not in removed/", f)
		}
	}
	if !exists(filepath.Join(fx.coins, "dgb.json")) {
		t.Error("another coin's config was touched")
	}
	// It asked again before deciding (1 + 3 retries, 2 calls each).
	if fx.docker.calls != 8 {
		t.Errorf("Docker asked %d times, want 8", fx.docker.calls)
	}
	if !strings.Contains(strings.Join(fx.logs, "\n"), "moved bch.json, bch_sv2.key, bch_sv2_authority.key into removed/") {
		t.Errorf("log: %v", fx.logs)
	}
}

func TestOwnerGoneReplacesOlderRemovedCopy(t *testing.T) {
	fx := newOwnerFixture(t, true)
	os.MkdirAll(filepath.Join(fx.coins, RemovedDirName), 0755)
	os.WriteFile(filepath.Join(fx.coins, RemovedDirName, "bch.json"), []byte("old"), 0644)
	fx.writeCoin(t, "bch", false)
	fx.settle("bch", "forgebch")
	b, _ := os.ReadFile(filepath.Join(fx.coins, RemovedDirName, "bch.json"))
	if string(b) != `{"owner_app":"x"}` {
		t.Fatalf("removed/bch.json = %q, want the newest config", b)
	}
}

func TestOwnerUninstalledKeepDataOnForgeNX(t *testing.T) {
	// "Uninstall, keep data" leaves the folder with only data/ in it.
	fx := newOwnerFixture(t, true)
	os.MkdirAll(filepath.Join(fx.apps, "forgebch", "data", ".bitcoin"), 0755)
	fx.writeCoin(t, "bch", false)
	if fx.settle("bch", "forgebch") || !exists(filepath.Join(fx.coins, RemovedDirName, "bch.json")) {
		t.Fatal("config of a keep-data uninstall was not moved aside")
	}
}

func TestOwnerUnknownWithoutDocker(t *testing.T) {
	// umbrelOS-like (no ForgeNX folder) and Docker can't be reached: keep.
	fx := newOwnerFixture(t, false)
	fx.writeCoin(t, "bch", true)
	if !fx.settle("bch", "forgebch") || !exists(filepath.Join(fx.coins, "bch.json")) {
		t.Fatal("config moved although Docker couldn't be asked")
	}
	if exists(filepath.Join(fx.coins, RemovedDirName)) {
		t.Fatal("removed/ created")
	}
}

func TestOwnerUnknownOnDockerError(t *testing.T) {
	fx := newOwnerFixture(t, true)
	fx.docker.status = http.StatusInternalServerError
	fx.writeCoin(t, "bch", false)
	if !fx.settle("bch", "forgebch") || !exists(filepath.Join(fx.coins, "bch.json")) {
		t.Fatal("config moved on a Docker error")
	}
}

func TestOwnerUnknownOnOddName(t *testing.T) {
	fx := newOwnerFixture(t, true)
	fx.writeCoin(t, "bch", false)
	if !fx.settle("bch", "../forgebch") || !exists(filepath.Join(fx.coins, "bch.json")) {
		t.Fatal("config moved for an owner_app that isn't a plain app name")
	}
}

func TestOwnerAppearsDuringRetries(t *testing.T) {
	// After a reboot the app shows up a moment after the engine starts.
	fx := newOwnerFixture(t, true)
	ownerRetries, ownerRetryWait = 40, 5*time.Millisecond
	fx.writeCoin(t, "bch", false)
	go func() {
		time.Sleep(30 * time.Millisecond)
		os.MkdirAll(filepath.Join(fx.apps, "forgebch"), 0755)
		os.WriteFile(filepath.Join(fx.apps, "forgebch", "docker-compose.yml"), nil, 0644)
	}()
	if !fx.settle("bch", "forgebch") || !exists(filepath.Join(fx.coins, "bch.json")) {
		t.Fatal("config moved although the app appeared during the retries")
	}
}
