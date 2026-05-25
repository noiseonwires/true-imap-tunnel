package main

import (
	"context"
	"encoding/base64"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/true-imap-tunnel/true-imap-tunnel/internal/config"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/diag"
)

func TestParsePluginOptionsEscapes(t *testing.T) {
	got := parsePluginOptions(`imap_host=imap.example.com:993;password=a\=b\;c\\d;mode=server`)
	if got["imap_host"] != "imap.example.com:993" {
		t.Fatalf("imap_host = %q", got["imap_host"])
	}
	if got["password"] != `a=b;c\d` {
		t.Fatalf("password = %q", got["password"])
	}
	if got["mode"] != "server" {
		t.Fatalf("mode = %q", got["mode"])
	}
}

func TestParsePluginOptionsFlagOption(t *testing.T) {
	got := parsePluginOptions("config=abc123;__android_vpn")
	if got["config"] != "abc123" {
		t.Fatalf("config = %q", got["config"])
	}
	if _, ok := got["__android_vpn"]; !ok {
		t.Fatalf("__android_vpn flag was not preserved")
	}
}

func TestBuildClientVersionUsesBuildMetadata(t *testing.T) {
	oldVersion, oldHash, oldDate := buildVersion, buildHash, buildDate
	defer func() {
		buildVersion, buildHash, buildDate = oldVersion, oldHash, oldDate
	}()
	buildVersion = "0.1.0"
	buildHash = "abc1234"
	buildDate = "20260524-111213"

	got := buildClientVersion()
	want := "true-imap-tunnel/0.1.0 hash=abc1234 date=20260524-111213"
	if got != want {
		t.Fatalf("buildClientVersion() = %q, want %q", got, want)
	}
}

func TestLoadSIP003ConfigClient(t *testing.T) {
	t.Setenv("SS_LOCAL_HOST", "127.0.0.1")
	t.Setenv("SS_LOCAL_PORT", "1081")
	t.Setenv("SS_PLUGIN_OPTIONS", "imap_host=imap.example.com:993;imap_username=u;imap_password=p;folder_send=c2s;folder_recv=s2c;encryption_passphrase=secret;client_encryption_passphrases=7:seven,8:eight;client_id=7;client_version=test-build;message_subject=Hello;message_subject_mode=random;subject_client_id=false")

	cfg, err := loadSIP003Config()
	if err != nil {
		t.Fatalf("loadSIP003Config: %v", err)
	}
	if cfg.Mode != config.ModeClient {
		t.Fatalf("mode = %q", cfg.Mode)
	}
	if cfg.Listen != "127.0.0.1:1081" {
		t.Fatalf("listen = %q", cfg.Listen)
	}
	if !cfg.AsyncDataSendEnabled() {
		t.Fatalf("async_data_send should default true in SIP003 mode")
	}
	if cfg.ZeroRTTOpen == nil || *cfg.ZeroRTTOpen {
		t.Fatalf("zero_rtt_open should default false in SIP003 mode")
	}
	if cfg.ClientID != 7 {
		t.Fatalf("client_id = %d", cfg.ClientID)
	}
	if cfg.ClientVersion != "test-build" {
		t.Fatalf("client_version = %q", cfg.ClientVersion)
	}
	if cfg.MessageSubject != "Hello" || cfg.EffectiveMessageSubjectMode() != config.MessageSubjectModeRandom {
		t.Fatalf("message subject config = %q/%q", cfg.MessageSubject, cfg.EffectiveMessageSubjectMode())
	}
	if cfg.SubjectClientID == nil || cfg.SubjectClientIDEnabled() {
		t.Fatalf("subject_client_id = %v, want false", cfg.SubjectClientID)
	}
	if cfg.ClientEncryptionPassphrases[7] != "seven" || cfg.ClientEncryptionPassphrases[8] != "eight" {
		t.Fatalf("client_encryption_passphrases = %#v", cfg.ClientEncryptionPassphrases)
	}
	if cfg.StatusAddr != diag.DefaultAndroidAddr {
		t.Fatalf("status_addr = %q, want %q", cfg.StatusAddr, diag.DefaultAndroidAddr)
	}
	if len(cfg.Accounts) != 1 || cfg.Accounts[0].FolderSend != "c2s" {
		t.Fatalf("account = %+v", cfg.Accounts)
	}
}

func TestDNSOverrideServersUsesExplicitConfig(t *testing.T) {
	cfg := &config.Config{
		DNSServers: []string{"8.8.8.8", "2001:4860:4860::8888", "9.9.9.9:5353"},
	}
	lookupCalled := false

	servers, reason, err := dnsOverrideServers(cfg, func(context.Context, string) ([]string, error) {
		lookupCalled = true
		return nil, nil
	})
	if err != nil {
		t.Fatalf("dnsOverrideServers: %v", err)
	}
	want := []string{"8.8.8.8:53", "[2001:4860:4860::8888]:53", "9.9.9.9:5353"}
	if strings.Join(servers, ",") != strings.Join(want, ",") {
		t.Fatalf("servers = %v, want %v", servers, want)
	}
	if reason != "configured" {
		t.Fatalf("reason = %q, want configured", reason)
	}
	if lookupCalled {
		t.Fatal("lookup should not be called for explicit DNS config")
	}
}

func TestDNSOverrideServersDefaultsToCloudflareWhenLocalFails(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeClient,
		Accounts: []config.AccountConfig{
			{Host: "imap.example.com:993"},
		},
	}

	servers, reason, err := dnsOverrideServers(cfg, func(_ context.Context, host string) ([]string, error) {
		if host != "imap.example.com" {
			t.Fatalf("probe host = %q, want imap.example.com", host)
		}
		return nil, errors.New("local resolver failed")
	})
	if err != nil {
		t.Fatalf("dnsOverrideServers: %v", err)
	}
	if len(servers) != 1 || servers[0] != defaultDNSFallbackServer {
		t.Fatalf("servers = %v, want [%s]", servers, defaultDNSFallbackServer)
	}
	if !strings.Contains(reason, "local resolver failed") {
		t.Fatalf("reason = %q, want local resolver failure", reason)
	}
}

func TestDNSOverrideServersDoesNothingWhenLocalWorks(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeClient,
		Accounts: []config.AccountConfig{
			{Host: "imap.example.com:993"},
		},
	}

	servers, reason, err := dnsOverrideServers(cfg, func(context.Context, string) ([]string, error) {
		return []string{"203.0.113.10"}, nil
	})
	if err != nil {
		t.Fatalf("dnsOverrideServers: %v", err)
	}
	if len(servers) != 0 || reason != "" {
		t.Fatalf("servers, reason = %v, %q; want no override", servers, reason)
	}
}

func TestDNSProbeHostSkipsIPAddresses(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:993", "[2001:db8::1]:993", "2001:db8::1"} {
		if got := dnsProbeHost(addr); got != "" {
			t.Fatalf("dnsProbeHost(%q) = %q, want empty", addr, got)
		}
	}
	if got := dnsProbeHost("imap.example.com:993"); got != "imap.example.com" {
		t.Fatalf("dnsProbeHost(host:port) = %q, want imap.example.com", got)
	}
}

func TestLoadSIP003ConfigMultipathMode(t *testing.T) {
	t.Setenv("SS_LOCAL_HOST", "127.0.0.1")
	t.Setenv("SS_LOCAL_PORT", "1081")
	t.Setenv("SS_PLUGIN_OPTIONS", "imap_host=imap.example.com:993;imap_username=u;imap_password=p;folder_send=c2s;folder_recv=s2c;multipath_mode=frame_round_robin")

	cfg, err := loadSIP003Config()
	if err != nil {
		t.Fatalf("loadSIP003Config: %v", err)
	}
	if cfg.EffectiveMultipathMode() != config.MultipathModeFrameRoundRobin {
		t.Fatalf("multipath_mode = %q", cfg.EffectiveMultipathMode())
	}
}

func TestLoadSIP003ConfigSSPortOverridesClientID(t *testing.T) {
	t.Setenv("SS_LOCAL_HOST", "127.0.0.1")
	t.Setenv("SS_LOCAL_PORT", "1081")
	t.Setenv("SS_PORT", "8388")
	t.Setenv("SS_PLUGIN_OPTIONS", "imap_host=imap.example.com:993;imap_username=u;imap_password=p;folder_send=c2s;folder_recv=s2c;client_id=7")

	cfg, err := loadSIP003Config()
	if err != nil {
		t.Fatalf("loadSIP003Config: %v", err)
	}
	if cfg.ClientID != 196 {
		t.Fatalf("client_id = %d", cfg.ClientID)
	}
}

func TestLoadSIP003ConfigSSPortOverridesYAMLClientID(t *testing.T) {
	t.Setenv("SS_LOCAL_HOST", "127.0.0.1")
	t.Setenv("SS_LOCAL_PORT", "1082")
	t.Setenv("SS_PORT", "8443")
	yaml := []byte(`mode: client
listen: "0.0.0.0:1"
client_id: 7
accounts:
  - host: "imap.example.com:993"
    username: "u"
    password: "p"
    folder_send: "c2s"
    folder_recv: "s2c"
`)
	t.Setenv("SS_PLUGIN_OPTIONS", "config="+base64.RawURLEncoding.EncodeToString(yaml))

	cfg, err := loadSIP003Config()
	if err != nil {
		t.Fatalf("loadSIP003Config: %v", err)
	}
	if cfg.ClientID != 251 {
		t.Fatalf("client_id = %d", cfg.ClientID)
	}
}

func TestLoadSIP003ConfigRemotePortOverridesYAMLClientID(t *testing.T) {
	t.Setenv("SS_LOCAL_HOST", "127.0.0.1")
	t.Setenv("SS_LOCAL_PORT", "1082")
	t.Setenv("SS_REMOTE_PORT", "443")
	yaml := []byte(`mode: client
listen: "0.0.0.0:1"
client_id: 7
accounts:
  - host: "imap.example.com:993"
    username: "u"
    password: "p"
    folder_send: "c2s"
    folder_recv: "s2c"
`)
	t.Setenv("SS_PLUGIN_OPTIONS", "config="+base64.RawURLEncoding.EncodeToString(yaml))

	cfg, err := loadSIP003Config()
	if err != nil {
		t.Fatalf("loadSIP003Config: %v", err)
	}
	if cfg.ClientID != 187 {
		t.Fatalf("client_id = %d", cfg.ClientID)
	}
}

func TestLoadSIP003ConfigSSPortTakesPrecedenceOverRemotePort(t *testing.T) {
	t.Setenv("SS_LOCAL_HOST", "127.0.0.1")
	t.Setenv("SS_LOCAL_PORT", "1081")
	t.Setenv("SS_PORT", "8388")
	t.Setenv("SS_REMOTE_PORT", "443")
	t.Setenv("SS_PLUGIN_OPTIONS", "imap_host=imap.example.com:993;imap_username=u;imap_password=p;folder_send=c2s;folder_recv=s2c;client_id=7")

	cfg, err := loadSIP003Config()
	if err != nil {
		t.Fatalf("loadSIP003Config: %v", err)
	}
	if cfg.ClientID != 196 {
		t.Fatalf("client_id = %d", cfg.ClientID)
	}
}

func TestLoadSIP003ConfigExplicitZeroRTTNonAndroid(t *testing.T) {
	t.Setenv("SS_LOCAL_HOST", "127.0.0.1")
	t.Setenv("SS_LOCAL_PORT", "1081")
	t.Setenv("SS_PLUGIN_OPTIONS", "imap_host=imap.example.com:993;imap_username=u;imap_password=p;folder_send=c2s;folder_recv=s2c;zero_rtt_open=true")

	cfg, err := loadSIP003Config()
	if err != nil {
		t.Fatalf("loadSIP003Config: %v", err)
	}
	if cfg.ZeroRTTOpen == nil || !*cfg.ZeroRTTOpen {
		t.Fatalf("explicit zero_rtt_open=true should be honored outside Android VPN mode")
	}
}

func TestLoadSIP003ConfigAndroidVPNHonorsExplicitZeroRTTFromYAML(t *testing.T) {
	t.Setenv("SS_LOCAL_HOST", "127.0.0.1")
	t.Setenv("SS_LOCAL_PORT", "1082")
	yaml := []byte(`mode: client
listen: "0.0.0.0:1"
zero_rtt_open: true
accounts:
  - host: "imap.example.com:993"
    username: "u"
    password: "p"
    folder_send: "c2s"
    folder_recv: "s2c"
`)
	t.Setenv("SS_PLUGIN_OPTIONS", "config="+base64.RawURLEncoding.EncodeToString(yaml)+";__android_vpn")

	cfg, err := loadSIP003Config()
	if err != nil {
		t.Fatalf("loadSIP003Config: %v", err)
	}
	if cfg.ZeroRTTOpen == nil || !*cfg.ZeroRTTOpen {
		t.Fatalf("Android VPN mode should honor explicit zero_rtt_open=true")
	}
}

func TestLoadSIP003ConfigTitsURLWithAndroidVPNFlag(t *testing.T) {
	t.Setenv("SS_LOCAL_HOST", "127.0.0.1")
	t.Setenv("SS_LOCAL_PORT", "1082")
	yaml := []byte(`mode: client
listen: "0.0.0.0:1"
zero_rtt_open: true
accounts:
  - host: "imap.example.com:993"
    username: "u"
    password: "p"
    folder_send: "c2s"
    folder_recv: "s2c"
`)
	t.Setenv("SS_PLUGIN_OPTIONS", "tits://"+base64.RawURLEncoding.EncodeToString(yaml)+";__android_vpn")

	cfg, err := loadSIP003Config()
	if err != nil {
		t.Fatalf("loadSIP003Config: %v", err)
	}
	if cfg.Listen != "127.0.0.1:1082" {
		t.Fatalf("listen = %q", cfg.Listen)
	}
	if cfg.ZeroRTTOpen == nil || !*cfg.ZeroRTTOpen {
		t.Fatalf("Android VPN mode should honor explicit zero_rtt_open=true")
	}
}

func TestLoadSIP003ConfigServer(t *testing.T) {
	t.Setenv("SS_LOCAL_HOST", "127.0.0.1")
	t.Setenv("SS_LOCAL_PORT", "8388")
	t.Setenv("SS_PLUGIN_OPTIONS", "mode=server;imap_host=imap.example.com:993;imap_username=u;imap_password=p;folder_send=s2c;folder_recv=c2s")

	cfg, err := loadSIP003Config()
	if err != nil {
		t.Fatalf("loadSIP003Config: %v", err)
	}
	if cfg.Mode != config.ModeServer {
		t.Fatalf("mode = %q", cfg.Mode)
	}
	if cfg.Target != "127.0.0.1:8388" {
		t.Fatalf("target = %q", cfg.Target)
	}
}

func TestLoadSIP003ConfigBase64YAML(t *testing.T) {
	t.Setenv("SS_LOCAL_HOST", "127.0.0.1")
	t.Setenv("SS_LOCAL_PORT", "1082")
	yaml := []byte(`mode: client
listen: "0.0.0.0:1"
accounts:
  - host: "imap.example.com:993"
    username: "u"
    password: "p"
    folder_send: "c2s"
    folder_recv: "s2c"
`)
	t.Setenv("SS_PLUGIN_OPTIONS", "config="+base64.RawURLEncoding.EncodeToString(yaml))

	cfg, err := loadSIP003Config()
	if err != nil {
		t.Fatalf("loadSIP003Config: %v", err)
	}
	if cfg.Listen != "127.0.0.1:1082" {
		t.Fatalf("listen = %q", cfg.Listen)
	}
	if cfg.Accounts[0].Host != "imap.example.com:993" {
		t.Fatalf("host = %q", cfg.Accounts[0].Host)
	}
}

func TestMakeSSURL(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "client.yaml")
	yaml := `# generated comments should not be embedded
mode: client
listen: "127.0.0.1:1080" # local listen
accounts:
  - host: "imap.example.com:993"
    username: "u"
    password: "p"
    folder_send: "c2s"
    folder_recv: "s2c"
`
	if err := os.WriteFile(path, []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	oldConfig, oldHost, oldPort := *flagConfig, *flagSSHost, *flagSSPort
	oldMethod, oldPassword := *flagSSMethod, *flagSSPassword
	defer func() {
		*flagConfig, *flagSSHost, *flagSSPort = oldConfig, oldHost, oldPort
		*flagSSMethod, *flagSSPassword = oldMethod, oldPassword
	}()
	*flagConfig = path
	*flagSSHost = "ss.example.com"
	*flagSSPort = "8388"
	*flagSSMethod = "aes-128-gcm"
	*flagSSPassword = "secret"
	u, err := makeSSURL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(u)
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}
	if parsed.Path != "" {
		t.Fatalf("path = %q, want Android-compatible empty path", parsed.Path)
	}
	wantUser := base64.RawURLEncoding.EncodeToString([]byte("aes-128-gcm:secret"))
	if parsed.User.String() != wantUser {
		t.Fatalf("userinfo = %q", parsed.User.String())
	}
	if !strings.Contains(u, "plugin=true-imap-tunnel") || !strings.Contains(u, "config%3D") {
		t.Fatalf("unexpected URL: %s", u)
	}
	plugin := parsed.Query().Get("plugin")
	opts := parsePluginOptions(strings.TrimPrefix(plugin, "true-imap-tunnel;"))
	data, err := base64.RawURLEncoding.DecodeString(opts["config"])
	if err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if strings.Contains(string(data), "#") {
		t.Fatalf("generated base64 config still contains comments:\n%s", data)
	}
	if _, err := config.LoadBytes(data); err != nil {
		t.Fatalf("generated config did not reload: %v\n%s", err, data)
	}
}

func TestMakeSSURLBase64SkipsDefaults(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "client.yaml")
	yaml := `mode: client
listen: "127.0.0.1:1080"
message_format: attachment
ping_interval_ms: 0
accounts:
  - host: "imap.example.com:993"
    username: "u"
    password: "p"
    tls: implicit
    folder_send: "c2s"
    folder_recv: "s2c"
`
	if err := os.WriteFile(path, []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	oldConfig, oldHost, oldPort := *flagConfig, *flagSSHost, *flagSSPort
	oldMethod, oldPassword := *flagSSMethod, *flagSSPassword
	oldFormat, oldSkip := *flagSSURLFormat, *flagSSURLSkipDefaults
	defer func() {
		*flagConfig, *flagSSHost, *flagSSPort = oldConfig, oldHost, oldPort
		*flagSSMethod, *flagSSPassword = oldMethod, oldPassword
		*flagSSURLFormat, *flagSSURLSkipDefaults = oldFormat, oldSkip
	}()
	*flagConfig = path
	*flagSSHost = "ss.example.com"
	*flagSSPort = "8388"
	*flagSSMethod = "aes-128-gcm"
	*flagSSPassword = "secret"
	*flagSSURLFormat = "base64"
	*flagSSURLSkipDefaults = true

	u, err := makeSSURL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	plugin := mustSSPluginOptions(t, u)
	opts := parsePluginOptions(strings.TrimPrefix(plugin, "true-imap-tunnel;"))
	data, err := base64.RawURLEncoding.DecodeString(opts["config"])
	if err != nil {
		t.Fatalf("decode config: %v", err)
	}
	text := string(data)
	for _, omitted := range []string{"message_format", "message_subject", "subject_client_id", "ping_interval_ms", "batch_delay_ms", "tls:"} {
		if strings.Contains(text, omitted) {
			t.Fatalf("default key %q was not skipped:\n%s", omitted, text)
		}
	}
	if _, err := config.LoadBytes(data); err != nil {
		t.Fatalf("generated compact config did not reload: %v\n%s", err, data)
	}
}

func TestMakeSSURLQueryMultiAccountAndParser(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "client.yaml")
	yaml := `mode: client
listen: "127.0.0.1:1080"
message_format: attachment
batch_delay_ms: 0
accounts:
  - host: "imap1.example.com:993"
    username: "u1"
    password: "p1"
    tls: implicit
    folder_send: "c2s-a"
    folder_recv: "s2c-a"
  - host: "imap2.example.com:993"
    username: "u2"
    password: "p2"
    insecure_skip_verify: true
    folder_send: "c2s-b"
    folder_recv: "s2c-b"
`
	if err := os.WriteFile(path, []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	oldConfig, oldHost, oldPort := *flagConfig, *flagSSHost, *flagSSPort
	oldMethod, oldPassword := *flagSSMethod, *flagSSPassword
	oldFormat, oldSkip := *flagSSURLFormat, *flagSSURLSkipDefaults
	defer func() {
		*flagConfig, *flagSSHost, *flagSSPort = oldConfig, oldHost, oldPort
		*flagSSMethod, *flagSSPassword = oldMethod, oldPassword
		*flagSSURLFormat, *flagSSURLSkipDefaults = oldFormat, oldSkip
	}()
	*flagConfig = path
	*flagSSHost = "ss.example.com"
	*flagSSPort = "8388"
	*flagSSMethod = "aes-128-gcm"
	*flagSSPassword = "secret"
	*flagSSURLFormat = "query"
	*flagSSURLSkipDefaults = true

	u, err := makeSSURL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	plugin := mustSSPluginOptions(t, u)
	if !strings.Contains(plugin, "imap_host_2=imap2.example.com:993") {
		t.Fatalf("missing second account suffix in plugin options: %s", plugin)
	}
	if !strings.Contains(plugin, "imap_insecure_skip_verify_2=true") {
		t.Fatalf("missing second account bool suffix in plugin options: %s", plugin)
	}
	if !strings.Contains(plugin, "batch_delay_ms=0") {
		t.Fatalf("missing explicit batch_delay_ms=0 in plugin options: %s", plugin)
	}
	if strings.Contains(plugin, "message_format=attachment") || strings.Contains(plugin, "imap_tls=implicit") {
		t.Fatalf("default values were not skipped: %s", plugin)
	}

	t.Setenv("SS_LOCAL_HOST", "127.0.0.1")
	t.Setenv("SS_LOCAL_PORT", "1081")
	t.Setenv("SS_PLUGIN_OPTIONS", strings.TrimPrefix(plugin, "true-imap-tunnel;"))
	loaded, err := loadSIP003Config()
	if err != nil {
		t.Fatalf("load generated query options: %v", err)
	}
	if len(loaded.Accounts) != 2 {
		t.Fatalf("accounts = %d", len(loaded.Accounts))
	}
	if loaded.Accounts[1].Host != "imap2.example.com:993" || loaded.Accounts[1].FolderRecv != "s2c-b" || !loaded.Accounts[1].InsecureSkipVerify {
		t.Fatalf("second account = %+v", loaded.Accounts[1])
	}
	if loaded.AsyncDataSendEnabled() {
		t.Fatalf("generated query should preserve YAML async_data_send=false")
	}
	if got := loaded.BatchDelay(); got != 0 {
		t.Fatalf("generated query should preserve explicit batch_delay_ms=0, got %v", got)
	}
}

func mustSSPluginOptions(t *testing.T, rawURL string) string {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}
	plugin := parsed.Query().Get("plugin")
	if plugin == "" {
		t.Fatalf("URL has no plugin query: %s", rawURL)
	}
	return plugin
}

func TestMakeSSURLDefaultsPlaceholderAuthority(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "client.yaml")
	yaml := `mode: client
listen: "127.0.0.1:1080"
accounts:
  - host: "imap.example.com:993"
    username: "u"
    password: "p"
    folder_send: "c2s"
    folder_recv: "s2c"
`
	if err := os.WriteFile(path, []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	oldConfig, oldHost, oldPort := *flagConfig, *flagSSHost, *flagSSPort
	oldMethod, oldPassword := *flagSSMethod, *flagSSPassword
	defer func() {
		*flagConfig, *flagSSHost, *flagSSPort = oldConfig, oldHost, oldPort
		*flagSSMethod, *flagSSPassword = oldMethod, oldPassword
	}()
	*flagConfig = path
	*flagSSHost = ""
	*flagSSPort = ""
	*flagSSMethod = "aes-128-gcm"
	*flagSSPassword = "secret"

	u, err := makeSSURL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(u, "@"+defaultSSURLHost+":"+defaultSSURLPort+"?") {
		t.Fatalf("unexpected URL authority: %s", u)
	}
}

func TestMakeSSURLRejects2022MethodsForAndroidImport(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "client.yaml")
	yaml := `mode: client
listen: "127.0.0.1:1080"
accounts:
  - host: "imap.example.com:993"
    username: "u"
    password: "p"
    folder_send: "c2s"
    folder_recv: "s2c"
`
	if err := os.WriteFile(path, []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	oldConfig, oldHost, oldPort := *flagConfig, *flagSSHost, *flagSSPort
	oldMethod, oldPassword := *flagSSMethod, *flagSSPassword
	defer func() {
		*flagConfig, *flagSSHost, *flagSSPort = oldConfig, oldHost, oldPort
		*flagSSMethod, *flagSSPassword = oldMethod, oldPassword
	}()
	*flagConfig = path
	*flagSSHost = "ss.example.com"
	*flagSSPort = "8388"
	*flagSSMethod = "2022-blake3-aes-128-gcm"
	*flagSSPassword = "secret"

	_, err = makeSSURL(cfg)
	if err == nil || !strings.Contains(err.Error(), "does not support 2022-*") {
		t.Fatalf("err = %v", err)
	}
}
