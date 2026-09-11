package tunnel

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emersion/go-imap/v2/imapclient"
	"github.com/noiseonwires/true-imap-tunnel/internal/config"
)

type httpSmokeResult struct {
	Provider             string          `json:"provider"`
	SmallRequests        int             `json:"small_requests"`
	ConcurrentRequests   int             `json:"concurrent_requests"`
	LatencyP50Ms         float64         `json:"latency_p50_ms"`
	LatencyP95Ms         float64         `json:"latency_p95_ms"`
	LatencyMaxMs         float64         `json:"latency_max_ms"`
	DownloadBytes        int64           `json:"download_bytes"`
	DownloadSeconds      float64         `json:"download_seconds"`
	DownloadMiBPerSecond float64         `json:"download_mib_per_second"`
	SHA256               string          `json:"sha256"`
	AsyncData            bool            `json:"async_data"`
	ClientAccounts       []AccountStatus `json:"client_accounts"`
	ServerAccounts       []AccountStatus `json:"server_accounts"`
}

func TestRealIMAPHTTP(t *testing.T) {
	host, user, password := os.Getenv("TIT_IMAP_HOST"), os.Getenv("TIT_IMAP_USER"), os.Getenv("TIT_IMAP_PASSWORD")
	if host == "" || user == "" || password == "" {
		t.Skip("opt-in: set TIT_IMAP_HOST, TIT_IMAP_USER, TIT_IMAP_PASSWORD")
	}
	if !strings.Contains(host, ":") {
		host = net.JoinHostPort(host, "993")
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	prefix := "TITSmoke" + hex.EncodeToString(nonce[:])
	c2s, s2c := prefix+"C2S", prefix+"S2C"
	admin, err := imapclient.DialTLS(host, &imapclient.Options{TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}})
	if err != nil {
		t.Fatal("smoke IMAP TLS connection failed")
	}
	defer admin.Close()
	if err := admin.Login(user, password).Wait(); err != nil {
		t.Fatal("smoke IMAP authentication failed")
	}
	var created []string
	defer func() {
		cleanup := admin
		if len(created) == 2 {
			var err error
			cleanup, err = imapclient.DialTLS(host, nil)
			if err != nil {
				t.Error("smoke cleanup reconnect failed")
				return
			}
			defer cleanup.Close()
			if err := cleanup.Login(user, password).Wait(); err != nil {
				t.Error("smoke cleanup authentication failed")
				return
			}
		}
		for _, folder := range created {
			if err := cleanup.Delete(folder).Wait(); err != nil {
				t.Errorf("could not remove smoke folder %s", folder)
			}
		}
		t.Log("isolated IMAP folders removed")
	}()
	for _, folder := range []string{c2s, s2c} {
		if err := admin.Create(folder, nil).Wait(); err != nil {
			t.Fatalf("cannot create isolated smoke folder %s", folder)
		}
		created = append(created, folder)
	}
	_ = admin.Close()

	const fileBytes = 16 * 1024 * 1024
	block := make([]byte, 32*1024)
	for index := range block {
		block[index] = byte((index*31 + index/251) % 256)
	}
	wantHash := sha256.New()
	for offset := 0; offset < fileBytes; offset += len(block) {
		_, _ = wantHash.Write(block)
	}
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/large" {
			writer.Header().Set("Content-Length", fmt.Sprint(fileBytes))
			for offset := 0; offset < fileBytes; offset += len(block) {
				if _, err := writer.Write(block); err != nil {
					return
				}
			}
			return
		}
		_, _ = fmt.Fprintf(writer, "smoke:%s", request.URL.Query().Get("id"))
	}))
	defer target.Close()
	account := config.AccountConfig{Name: "real-smoke", Host: host, TLS: "implicit", Username: user, Password: password, FolderSend: c2s, FolderRecv: s2c}
	async := os.Getenv("TIT_SMOKE_ASYNC") == "1"
	clientCfg := &config.Config{Mode: config.ModeClient, Listen: freePort(t), Accounts: []config.AccountConfig{account}, AsyncDataSend: async, PingIntervalMs: -1}
	account.FolderSend, account.FolderRecv = s2c, c2s
	serverCfg := &config.Config{Mode: config.ModeServer, Target: strings.TrimPrefix(target.URL, "http://"), Accounts: []config.AccountConfig{account}, AsyncDataSend: async}
	client, server := mustNew(t, clientCfg), mustNew(t, serverCfg)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); _ = server.Run(ctx) }()
	go func() { defer workers.Done(); _ = client.Run(ctx) }()
	defer func() { cancel(); workers.Wait() }()
	readyDeadline := time.NewTimer(90 * time.Second)
	defer readyDeadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for !client.clientTransportReady() || !server.clientTransportReady() {
		select {
		case <-tick.C:
		case <-readyDeadline.C:
			t.Fatal("real IMAP transport did not become ready")
		}
	}
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport, Timeout: 15 * time.Minute}
	baseURL := "http://" + clientCfg.Listen
	requestSmall := func(id string) (time.Duration, error) {
		start := time.Now()
		requestCtx, stop := context.WithTimeout(ctx, 90*time.Second)
		defer stop()
		request, _ := http.NewRequestWithContext(requestCtx, http.MethodGet, baseURL+"/small?id="+id, nil)
		response, err := httpClient.Do(request)
		if err != nil {
			return 0, err
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return 0, err
		}
		if response.StatusCode != http.StatusOK || string(body) != "smoke:"+id {
			return 0, fmt.Errorf("small HTTP body/status mismatch")
		}
		return time.Since(start), nil
	}
	if _, err := requestSmall("warmup"); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	latencies := make([]float64, 100)
	for index := range latencies {
		elapsed, err := requestSmall(fmt.Sprint(index))
		if err != nil {
			t.Fatalf("small request %d: %v", index, err)
		}
		latencies[index] = float64(elapsed.Microseconds()) / 1000
		if (index+1)%20 == 0 {
			t.Logf("small HTTP requests: %d/100 verified", index+1)
		}
	}
	var concurrent sync.WaitGroup
	errors := make(chan error, 32)
	for worker := 0; worker < 8; worker++ {
		concurrent.Add(1)
		go func(worker int) {
			defer concurrent.Done()
			for index := 0; index < 4; index++ {
				_, err := requestSmall(fmt.Sprintf("parallel-%d-%d", worker, index))
				if err != nil {
					errors <- err
					return
				}
			}
		}(worker)
	}
	concurrent.Wait()
	close(errors)
	for err := range errors {
		t.Fatalf("concurrent HTTP: %v", err)
	}
	t.Log("32 concurrent HTTP requests verified; downloading 16 MiB")
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/large", nil)
	started := time.Now()
	response, err := httpClient.Do(request)
	if err != nil {
		t.Fatalf("large download: %v", err)
	}
	hash := sha256.New()
	count, copyErr := io.Copy(hash, response.Body)
	_ = response.Body.Close()
	elapsed := time.Since(started)
	if copyErr != nil || response.StatusCode != http.StatusOK || count != fileBytes || !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), hex.EncodeToString(wantHash.Sum(nil))) {
		t.Fatalf("large download integrity failure: bytes=%d status=%d error=%v", count, response.StatusCode, copyErr)
	}
	sort.Float64s(latencies)
	result := httpSmokeResult{Provider: host, SmallRequests: len(latencies), ConcurrentRequests: 32, LatencyP50Ms: latencies[49], LatencyP95Ms: latencies[94], LatencyMaxMs: latencies[99], DownloadBytes: count, DownloadSeconds: elapsed.Seconds(), DownloadMiBPerSecond: float64(count) / (1024 * 1024) / elapsed.Seconds(), SHA256: hex.EncodeToString(hash.Sum(nil)), AsyncData: async}
	result.ClientAccounts = client.paths.AccountStatuses()
	result.ServerAccounts = server.paths.AccountStatuses()
	encoded, _ := json.MarshalIndent(result, "", "  ")
	t.Logf("HTTP smoke result:\n%s", encoded)
	if path := os.Getenv("TIT_SMOKE_RESULT_OUT"); path != "" {
		if err := os.WriteFile(path, append(encoded, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
