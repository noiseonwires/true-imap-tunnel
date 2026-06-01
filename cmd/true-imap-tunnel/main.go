// Command true-imap-tunnel is a TCP tunnel that transports data through
// IMAP draft messages. See README and config.example.yaml for details.
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/true-imap-tunnel/true-imap-tunnel/internal/config"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/diag"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/netprotect"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/tlog"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/tunnel"
	"gopkg.in/yaml.v3"
)

// verbosityFlag implements flag.Value for repeated -v handling: each
// occurrence bumps the verbosity by one level. Compatible with -v -v -v.
type verbosityFlag int

func (v *verbosityFlag) String() string { return fmt.Sprintf("%d", int(*v)) }
func (v *verbosityFlag) Set(s string) error {
	// Bare -v with no value → bump by one (BoolFlag semantics).
	if s == "" || s == "true" {
		*v++
		return nil
	}
	// -v=2 / -v=trace → set explicitly.
	if lvl, ok := tlog.ParseLevel(s); ok {
		*v = verbosityFlag(int(lvl) - int(tlog.LevelInfo))
		return nil
	}
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err == nil {
		*v = verbosityFlag(n)
		return nil
	}
	return fmt.Errorf("invalid verbosity %q", s)
}
func (v *verbosityFlag) IsBoolFlag() bool { return true }

const (
	defaultSSURLHost         = "tits.local"
	defaultSSURLPort         = "443"
	defaultDNSFallbackServer = "1.1.1.1:53"
)

var (
	flagConfig            = flag.String("config", "config.yaml", "path to YAML configuration")
	flagConfigStdin       = flag.Bool("config-stdin", false, "read YAML configuration from stdin instead of -config")
	flagMode              = flag.String("mode", "", "override mode from config (client|server)")
	flagSIP003            = flag.Bool("sip003", false, "read SIP003 Shadowsocks plugin environment variables")
	flagShowSSURL         = flag.Bool("show-ss-url", false, "print a Shadowsocks ss:// URL embedding this YAML config")
	flagSSHost            = flag.String("ss-host", defaultSSURLHost, "Shadowsocks profile host for -show-ss-url; ignored by this plugin")
	flagSSPort            = flag.String("ss-port", defaultSSURLPort, "Shadowsocks profile port for -show-ss-url; SIP003 clients expose it as the client_id override port")
	flagSSMethod          = flag.String("ss-method", "", "Shadowsocks method for -show-ss-url")
	flagSSPassword        = flag.String("ss-password", "", "Shadowsocks password for -show-ss-url")
	flagSSPluginID        = flag.String("ss-plugin-id", "true-imap-tunnel", "plugin id for -show-ss-url")
	flagSSTag             = flag.String("ss-tag", "T.I.T.(s.)", "URL fragment/tag for -show-ss-url")
	flagSSURLFormat       = flag.String("ss-url-format", "base64", "plugin options format for -show-ss-url: base64 or query")
	flagSSURLSkipDefaults = flag.Bool("ss-url-skip-defaults", false, "omit optional config keys with default values from -show-ss-url output")
	flagShowConfig        = flag.Bool("show-config", false, "print the loaded configuration and exit")
	flagLogLevel          = flag.String("log-level", "", "log level (error|warn|info|debug|trace); overrides config")
	flagVerbose           verbosityFlag
)

// Set at build time via -ldflags.
var (
	buildVersion = "dev"
	buildDate    = "unknown"
	buildHash    = "unknown"
)

func init() {
	flag.Var(&flagVerbose, "v", "bump verbosity (-v debug, -v -v trace); also accepts -v=trace")
}

func buildClientVersion() string {
	version := strings.TrimSpace(buildVersion)
	if version == "" {
		version = "dev"
	}
	hash := strings.TrimSpace(buildHash)
	if hash == "" {
		hash = "unknown"
	}
	date := strings.TrimSpace(buildDate)
	if date == "" {
		date = "unknown"
	}
	return fmt.Sprintf("true-imap-tunnel/%s hash=%s date=%s", version, hash, date)
}

func main() {
	flag.Parse()

	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var cfg *config.Config
	var err error
	if *flagSIP003 || sip003EnvPresent() {
		cfg, err = loadSIP003Config()
		if err == nil {
			err = configureSIP003AndroidVPNProtection(os.Getenv("SS_PLUGIN_OPTIONS"))
		}
	} else if *flagConfigStdin {
		cfg, err = config.LoadReader(os.Stdin)
	} else {
		cfg, err = config.Load(*flagConfig)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}
	if *flagShowSSURL {
		u, err := makeSSURL(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
		fmt.Println(u)
		return
	}
	if *flagMode != "" {
		cfg.Mode = config.Mode(*flagMode)
		if err := cfg.Validate(); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
	}
	if strings.TrimSpace(cfg.ClientVersion) == "" {
		cfg.ClientVersion = buildClientVersion()
	}

	// Pick the log level. Precedence (highest first):
	//   1. -log-level=<name>
	//   2. -v repeated (each bump adds one level above the config / default)
	//   3. config.log_level
	//   4. INFO
	level := tlog.LevelInfo
	if cfg.LogLevel != "" {
		if l, ok := tlog.ParseLevel(cfg.LogLevel); ok {
			level = l
		} else {
			fmt.Fprintf(os.Stderr, "warning: unknown log_level %q; using info\n", cfg.LogLevel)
		}
	}
	if int(flagVerbose) != 0 {
		bumped := int(level) + int(flagVerbose)
		if bumped < int(tlog.LevelError) {
			bumped = int(tlog.LevelError)
		}
		if bumped > int(tlog.LevelTrace) {
			bumped = int(tlog.LevelTrace)
		}
		level = tlog.Level(bumped)
	}
	if *flagLogLevel != "" {
		if l, ok := tlog.ParseLevel(*flagLogLevel); ok {
			level = l
		} else {
			fmt.Fprintf(os.Stderr, "warning: unknown -log-level %q; using %s\n", *flagLogLevel, level)
		}
	}
	tlog.SetLevel(level)

	if *flagShowConfig {
		fmt.Printf("mode: %s\n", cfg.Mode)
		fmt.Printf("listen: %s\n", cfg.Listen)
		fmt.Printf("target: %s\n", cfg.Target)
		fmt.Printf("log_level: %s\n", level)
		fmt.Printf("message_format: %s\n", cfg.EffectiveMessageFormat())
		fmt.Printf("message_subject_mode: %s\n", cfg.EffectiveMessageSubjectMode())
		if cfg.EffectiveMessageSubjectMode() == config.MessageSubjectModeFixed {
			fmt.Printf("message_subject: %q\n", cfg.EffectiveMessageSubject())
		}
		fmt.Printf("message_to: %q\n", cfg.EffectiveMessageTo())
		fmt.Printf("subject_client_id: %t\n", cfg.SubjectClientIDEnabled())
		fmt.Printf("client_version: %s\n", cfg.ClientVersion)
		if diag.Disabled(cfg.StatusAddr) {
			fmt.Printf("status_addr: disabled\n")
		} else {
			fmt.Printf("status_addr: %s\n", cfg.StatusAddr)
		}
		if cfg.EffectiveMessageFormat() == "attachment" {
			fmt.Printf("attachment_filename: %q\n", cfg.EffectiveAttachmentFilename())
		}
		if cfg.EncryptionPassphrase != "" {
			fmt.Printf("encryption: AES-256-GCM (passphrase set)\n")
		} else if len(cfg.ClientEncryptionPassphrases) > 0 {
			fmt.Printf("encryption: AES-256-GCM (%d client key(s))\n", len(cfg.ClientEncryptionPassphrases))
		} else {
			fmt.Printf("encryption: disabled\n")
		}
		fmt.Printf("zero_rtt_open: %v\n", cfg.ZeroRTTOpenEnabled())
		switch {
		case !cfg.PingEnabled():
			fmt.Printf("ping: disabled\n")
		case cfg.PingPeriodic():
			fmt.Printf("ping: once at startup, then every %v\n", cfg.PingInterval())
		default:
			fmt.Printf("ping: single probe at startup\n")
		}
		fmt.Printf("accounts: %d\n", len(cfg.Accounts))
		for i, a := range cfg.Accounts {
			auth := "password"
			if a.UseOAuth2() {
				auth = "oauth2"
			}
			fmt.Printf("  [%d] %s host=%s user=%s tls=%s auth=%s send=%q recv=%q from=%q\n",
				i+1, a.Label(), a.Host, a.Username, a.TLS, auth, a.FolderSend, a.FolderRecv, a.EffectiveMessageFrom())
		}
		fmt.Printf("batch: max_frames=%d max_bytes=%d queue=%d delay=%v\n",
			cfg.BatchMaxFrames(), cfg.BatchMaxBytes(), cfg.BatchQueueSize(), cfg.BatchDelay())
		fmt.Printf("multipath_mode: %s\n", cfg.EffectiveMultipathMode())
		fmt.Printf("reorder: %v\n", cfg.ReorderEnabled())
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Signal handling: SIGINT/SIGTERM → cancel and let Tunnel.Run do
	// its graceful shutdown. Second signal forces immediate exit. A
	// background hard-exit timer caps total shutdown time at the
	// configured graceful_shutdown_ms plus a 2-second slack — beyond
	// that, a hung IMAP connection isn't worth keeping the process
	// around for.
	go func() {
		sigCh := make(chan os.Signal, 2)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		tlog.Infof("shutting down")
		cancel()
		go func() {
			<-sigCh
			tlog.Warnf("second signal — forcing exit")
			os.Exit(1)
		}()
		go func() {
			hardDeadline := cfg.GracefulShutdown() + 2*time.Second
			time.Sleep(hardDeadline)
			tlog.Warnf("hard shutdown deadline %v expired — forcing exit", hardDeadline)
			os.Exit(1)
		}()
	}()

	t, err := tunnel.New(cfg)
	if err != nil {
		tlog.Errorf("tunnel init: %v", err)
		os.Exit(1)
	}
	if !diag.Disabled(cfg.StatusAddr) {
		statusServer := diag.NewServer(cfg.StatusAddr, func() any { return t.StatusSnapshot() })
		if err := statusServer.Start(ctx); err != nil {
			tlog.Warnf("status API disabled: %v", err)
		} else {
			statusServer.InstallLogSink()
		}
	}

	// Optional custom DNS fallback. When unset, the system resolver is used.
	if err := applyDNSOverride(cfg); err != nil {
		tlog.Errorf("dns override: %v", err)
		os.Exit(1)
	}

	tlog.Infof("true-imap-tunnel starting mode=%s accounts=%d log_level=%s build=%s/%s",
		cfg.Mode, len(cfg.Accounts), level, buildDate, buildHash)
	tlog.Infof("copyright (C) 2026 Kirill aka noiseonwires https://github.com/noiseonwires")

	// PID file: kill any stale instance before we try to bind the
	// listen port, then write our own PID. Cleanup on exit.
	if cfg.PidFile != "" {
		killStalePid(cfg.PidFile)
		writePidFile(cfg.PidFile)
		defer os.Remove(cfg.PidFile)
	}

	if err := t.Run(ctx); err != nil {
		tlog.Errorf("tunnel: %v", err)
		os.Exit(1)
	}
	tlog.Infof("bye")
}

func sip003EnvPresent() bool {
	return os.Getenv("SS_LOCAL_HOST") != "" ||
		os.Getenv("SS_LOCAL_PORT") != "" ||
		os.Getenv("SS_PORT") != "" ||
		os.Getenv("SS_REMOTE_HOST") != "" ||
		os.Getenv("SS_REMOTE_PORT") != "" ||
		os.Getenv("SS_PLUGIN_OPTIONS") != ""
}

func loadSIP003Config() (*config.Config, error) {
	rawOptions := os.Getenv("SS_PLUGIN_OPTIONS")
	if cfg, ok, err := decodePluginOptionsConfig(rawOptions); ok || err != nil {
		if err != nil {
			return nil, err
		}
		applySIP003Endpoint(cfg)
		applySIP003ClientIDOverride(cfg)
		defaultSIP003StatusAddr(cfg)
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
		return cfg, nil
	}

	opts := parsePluginOptions(rawOptions)
	mode := config.Mode(optionString(opts, "mode", string(config.ModeClient)))
	localHost := envDefault("SS_LOCAL_HOST", "127.0.0.1")
	localPort := os.Getenv("SS_LOCAL_PORT")
	if localPort == "" {
		return nil, fmt.Errorf("SIP003 requires SS_LOCAL_PORT")
	}

	reorder := optionBool(opts, "reorder", true)
	zeroRTT := optionBool(opts, "zero_rtt_open", false)
	var subjectClientID *bool
	if _, ok := opts["subject_client_id"]; ok {
		v := optionBool(opts, "subject_client_id", true)
		subjectClientID = &v
	}
	clientPassphrases, err := parseClientEncryptionPassphrases(opts["client_encryption_passphrases"])
	if err != nil {
		return nil, err
	}
	_, batchDelaySet := opts["batch_delay_ms"]
	cfg := &config.Config{
		Mode:                    mode,
		LogLevel:                opts["log_level"],
		Reorder:                 &reorder,
		ZeroRTTOpen:             &zeroRTT,
		AsyncDataSend:           optionBool(opts, "async_data_send", true),
		BatchDelayMs:            optionInt(opts, "batch_delay_ms", 0),
		BatchDelayMsSet:         batchDelaySet,
		BatchMaxFrames_:         optionInt(opts, "batch_max_frames", 0),
		BatchMaxKB:              optionInt(opts, "batch_max_kb", 0),
		BatchQueueSize_:         optionInt(opts, "batch_queue_size", 0),
		InboundQueueSize_:       optionInt(opts, "inbound_queue_size", 0),
		InboundQueueWaitMs:      optionInt(opts, "inbound_queue_wait_ms", 0),
		OpenTimeoutSec:          optionInt(opts, "open_timeout_sec", 0),
		DialTimeoutSec:          optionInt(opts, "dial_timeout_sec", 0),
		ReconnectInitialDelayMs: optionInt(opts, "reconnect_initial_delay_ms", 0),
		ReconnectMaxDelayMs:     optionInt(opts, "reconnect_max_delay_ms", 0),
		ReconnectBackoff:        optionFloat(opts, "reconnect_backoff", 0),
		ThrottleBackoffMs:       optionInt(opts, "throttle_backoff_ms", 0),
		PollIntervalMs:          optionInt(opts, "poll_interval_ms", 0),
		ActivePollIntervalMs:    optionInt(opts, "active_poll_interval_ms", 0),
		ActivePollDurationMs:    optionInt(opts, "active_poll_duration_ms", 0),
		LazyExpungeThreshold_:   optionInt(opts, "lazy_expunge_threshold", 0),
		LazyExpungeMaxAgeMs:     optionInt(opts, "lazy_expunge_max_age_ms", 0),
		StartupCleanupConnection: config.StartupCleanupConnection(optionString(opts, "startup_cleanup_connection",
			string(config.StartupCleanupConnectionFallback))),
		PingIntervalMs:              optionInt(opts, "ping_interval_ms", 0),
		MessageFormat:               optionString(opts, "message_format", "attachment"),
		AttachmentFilename:          opts["attachment_filename"],
		MessageSubject:              opts["message_subject"],
		MessageSubjectMode:          config.MessageSubjectMode(optionString(opts, "message_subject_mode", string(config.MessageSubjectModeFixed))),
		MessageTo:                   opts["message_to"],
		SubjectClientID:             subjectClientID,
		MultipathMode:               config.MultipathMode(optionString(opts, "multipath_mode", string(config.MultipathModeStreamAffinity))),
		EncryptionPassphrase:        opts["encryption_passphrase"],
		ClientEncryptionPassphrases: clientPassphrases,
		PidFile:                     opts["pid_file"],
		GracefulShutdownMs:          optionInt(opts, "graceful_shutdown_ms", 0),
		ClientID:                    optionUint8(opts, "client_id", 0),
		ClientVersion:               opts["client_version"],
		StatusAddr:                  optionString(opts, "status_addr", diag.DefaultAndroidAddr),
	}
	if cfg.Mode == config.ModeClient {
		cfg.Listen = net.JoinHostPort(localHost, localPort)
	} else {
		cfg.Target = net.JoinHostPort(localHost, localPort)
	}
	if v := opts["dns_server"]; v != "" {
		cfg.DNSServers = []string{v}
	}
	if v := opts["dns_servers"]; v != "" {
		cfg.DNSServers = strings.Split(v, ",")
	}
	cfg.Accounts = parseSIP003Accounts(opts)
	applySIP003ClientIDOverride(cfg)
	defaultSIP003StatusAddr(cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func defaultSIP003StatusAddr(cfg *config.Config) {
	if strings.TrimSpace(cfg.StatusAddr) == "" {
		cfg.StatusAddr = diag.DefaultAndroidAddr
	}
}

func applySIP003Endpoint(cfg *config.Config) {
	localHost := envDefault("SS_LOCAL_HOST", "127.0.0.1")
	localPort := os.Getenv("SS_LOCAL_PORT")
	if localPort == "" {
		return
	}
	if cfg.Mode == config.ModeServer {
		cfg.Target = net.JoinHostPort(localHost, localPort)
	} else {
		cfg.Mode = config.ModeClient
		cfg.Listen = net.JoinHostPort(localHost, localPort)
	}
}

func applySIP003ClientIDOverride(cfg *config.Config) {
	if cfg.Mode != config.ModeClient {
		return
	}
	envName, port := sip003ClientIDOverridePort()
	if port == "" {
		return
	}
	id, err := clientIDFromPort(port)
	if err != nil {
		tlog.Warnf("ignoring %s client_id override: %v", envName, err)
		return
	}
	cfg.ClientID = id
	tlog.Infof("SIP003 client_id override from %s=%s -> %d", envName, port, id)
}

func sip003ClientIDOverridePort() (string, string) {
	if port := os.Getenv("SS_PORT"); port != "" {
		return "SS_PORT", port
	}
	if port := os.Getenv("SS_REMOTE_PORT"); port != "" {
		return "SS_REMOTE_PORT", port
	}
	return "", ""
}

func clientIDFromPort(port string) (uint8, error) {
	n, err := strconv.Atoi(strings.TrimSpace(port))
	if err != nil {
		return 0, fmt.Errorf("parse SIP003 port %q: %w", port, err)
	}
	if n <= 0 || n > 65535 {
		return 0, fmt.Errorf("SIP003 port %q outside valid TCP port range", port)
	}
	id := n % 256
	if id == 0 {
		id = 255
	}
	return uint8(id), nil
}

func parseSIP003Accounts(opts map[string]string) []config.AccountConfig {
	fields := []string{
		"name", "imap_host", "imap_username", "imap_password",
		"oauth2_token", "oauth2_token_command", "imap_tls", "imap_insecure_skip_verify",
		"folder_send", "folder_recv", "message_from",
	}
	var accounts []config.AccountConfig
	for idx := 0; ; idx++ {
		if !hasSIP003AccountOption(opts, idx, fields) {
			break
		}
		name := accountOptionString(opts, "name", idx, "")
		if name == "" {
			if idx == 0 {
				name = "shadowsocks"
			} else {
				name = fmt.Sprintf("shadowsocks_%d", idx+1)
			}
		}
		accounts = append(accounts, config.AccountConfig{
			Name:               name,
			Host:               accountOptionString(opts, "imap_host", idx, ""),
			Username:           accountOptionString(opts, "imap_username", idx, ""),
			Password:           accountOptionString(opts, "imap_password", idx, ""),
			OAuth2Token:        accountOptionString(opts, "oauth2_token", idx, ""),
			OAuth2TokenCommand: accountOptionString(opts, "oauth2_token_command", idx, ""),
			TLS:                accountOptionString(opts, "imap_tls", idx, "implicit"),
			InsecureSkipVerify: accountOptionBool(opts, "imap_insecure_skip_verify", idx, false),
			FolderSend:         accountOptionString(opts, "folder_send", idx, ""),
			FolderRecv:         accountOptionString(opts, "folder_recv", idx, ""),
			MessageFrom:        accountOptionString(opts, "message_from", idx, ""),
		})
	}
	return accounts
}

func hasSIP003AccountOption(opts map[string]string, idx int, fields []string) bool {
	for _, field := range fields {
		if _, ok := lookupAccountOption(opts, field, idx); ok {
			return true
		}
	}
	return false
}

func accountOptionString(opts map[string]string, key string, idx int, def string) string {
	if v, ok := lookupAccountOption(opts, key, idx); ok && v != "" {
		return v
	}
	return def
}

func accountOptionBool(opts map[string]string, key string, idx int, def bool) bool {
	if v, ok := lookupAccountOption(opts, key, idx); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func lookupAccountOption(opts map[string]string, key string, idx int) (string, bool) {
	if idx == 0 {
		if v, ok := opts[key]; ok {
			return v, true
		}
		if v, ok := opts[key+"_1"]; ok {
			return v, true
		}
		return "", false
	}
	v, ok := opts[fmt.Sprintf("%s_%d", key, idx+1)]
	return v, ok
}

func decodePluginOptionsConfig(raw string) (*config.Config, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false, nil
	}
	if strings.HasPrefix(raw, "tits://") {
		payload := strings.TrimPrefix(raw, "tits://")
		if i := strings.IndexByte(payload, ';'); i >= 0 {
			payload = payload[:i]
		}
		return decodeBase64YAMLConfig(payload)
	}
	opts := parsePluginOptions(raw)
	for _, key := range []string{"config", "config_b64", "yaml_b64"} {
		if v := opts[key]; v != "" {
			return decodeBase64YAMLConfig(v)
		}
	}
	return nil, false, nil
}

func decodeBase64YAMLConfig(s string) (*config.Config, bool, error) {
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return nil, true, fmt.Errorf("decode base64 YAML config: %w", err)
	}
	cfg, err := config.LoadBytes(data)
	if err != nil {
		return nil, true, err
	}
	return cfg, true, nil
}

func makeSSURL(cfg *config.Config) (string, error) {
	if *flagSSMethod == "" || *flagSSPassword == "" {
		return "", fmt.Errorf("-show-ss-url requires -ss-method and -ss-password")
	}
	if strings.HasPrefix(*flagSSMethod, "2022-") {
		return "", fmt.Errorf("-show-ss-url for Shadowsocks Android does not support 2022-* methods; use aes-128-gcm, aes-256-gcm, or chacha20-ietf-poly1305")
	}
	data, err := os.ReadFile(*flagConfig)
	if err != nil {
		return "", fmt.Errorf("read config for URL: %w", err)
	}
	// Validate the already-loaded config was useful; cfg is otherwise only
	// referenced so generation follows normal config loading/validation.
	if len(cfg.Accounts) == 0 {
		return "", fmt.Errorf("config has no accounts")
	}
	pluginOpts, err := makeSSPluginOptions(cfg, data)
	if err != nil {
		return "", err
	}
	plugin := *flagSSPluginID
	if pluginOpts != "" {
		plugin += ";" + pluginOpts
	}
	host := *flagSSHost
	if host == "" {
		host = defaultSSURLHost
	}
	port := *flagSSPort
	if port == "" {
		port = defaultSSURLPort
	}
	u := &url.URL{
		Scheme:   "ss",
		User:     ssURLUserinfo(*flagSSMethod, *flagSSPassword),
		Host:     net.JoinHostPort(host, port),
		RawQuery: "plugin=" + url.QueryEscape(plugin),
		Fragment: *flagSSTag,
	}
	return u.String(), nil
}

func makeSSPluginOptions(cfg *config.Config, rawYAML []byte) (string, error) {
	switch strings.ToLower(strings.TrimSpace(*flagSSURLFormat)) {
	case "", "base64", "b64", "yaml":
		data, err := ssURLYAMLConfig(cfg, rawYAML, *flagSSURLSkipDefaults)
		if err != nil {
			return "", err
		}
		return "config=" + base64.RawURLEncoding.EncodeToString(data), nil
	case "query", "inline":
		return ssURLQueryOptions(cfg, *flagSSURLSkipDefaults)
	default:
		return "", fmt.Errorf("invalid -ss-url-format %q (expected base64 or query)", *flagSSURLFormat)
	}
}

func ssURLYAMLConfig(cfg *config.Config, rawYAML []byte, skipDefaults bool) ([]byte, error) {
	if skipDefaults {
		return compactYAMLConfig(cfg)
	}
	data, err := stripYAMLComments(rawYAML)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func stripYAMLComments(data []byte) ([]byte, error) {
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return nil, fmt.Errorf("parse config for URL: %w", err)
	}
	clearYAMLComments(&node)
	out, err := yaml.Marshal(&node)
	if err != nil {
		return nil, fmt.Errorf("encode comment-free config for URL: %w", err)
	}
	return out, nil
}

func clearYAMLComments(node *yaml.Node) {
	if node == nil {
		return
	}
	node.HeadComment = ""
	node.LineComment = ""
	node.FootComment = ""
	for _, child := range node.Content {
		clearYAMLComments(child)
	}
}

type ssURLYAMLAccount struct {
	Name               string `yaml:"name,omitempty"`
	Host               string `yaml:"host"`
	Username           string `yaml:"username"`
	Password           string `yaml:"password,omitempty"`
	OAuth2Token        string `yaml:"oauth2_token,omitempty"`
	OAuth2TokenCommand string `yaml:"oauth2_token_command,omitempty"`
	TLS                string `yaml:"tls,omitempty"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify,omitempty"`
	FolderSend         string `yaml:"folder_send"`
	FolderRecv         string `yaml:"folder_recv"`
	MessageFrom        string `yaml:"message_from,omitempty"`
}

type ssURLYAMLConfigDoc struct {
	Mode                        config.Mode                     `yaml:"mode"`
	LogLevel                    string                          `yaml:"log_level,omitempty"`
	Listen                      string                          `yaml:"listen,omitempty"`
	Target                      string                          `yaml:"target,omitempty"`
	ClientID                    uint8                           `yaml:"client_id,omitempty"`
	ClientVersion               string                          `yaml:"client_version,omitempty"`
	StatusAddr                  string                          `yaml:"status_addr,omitempty"`
	Accounts                    []ssURLYAMLAccount              `yaml:"accounts"`
	Reorder                     *bool                           `yaml:"reorder,omitempty"`
	MultipathMode               config.MultipathMode            `yaml:"multipath_mode,omitempty"`
	OpenTimeoutSec              int                             `yaml:"open_timeout_sec,omitempty"`
	DialTimeoutSec              int                             `yaml:"dial_timeout_sec,omitempty"`
	ReconnectInitialDelayMs     int                             `yaml:"reconnect_initial_delay_ms,omitempty"`
	ReconnectMaxDelayMs         int                             `yaml:"reconnect_max_delay_ms,omitempty"`
	ReconnectBackoff            float64                         `yaml:"reconnect_backoff,omitempty"`
	ThrottleBackoffMs           int                             `yaml:"throttle_backoff_ms,omitempty"`
	PollIntervalMs              int                             `yaml:"poll_interval_ms,omitempty"`
	ActivePollIntervalMs        int                             `yaml:"active_poll_interval_ms,omitempty"`
	ActivePollDurationMs        int                             `yaml:"active_poll_duration_ms,omitempty"`
	LazyExpungeThreshold        int                             `yaml:"lazy_expunge_threshold,omitempty"`
	LazyExpungeMaxAgeMs         int                             `yaml:"lazy_expunge_max_age_ms,omitempty"`
	StartupCleanupConnection    config.StartupCleanupConnection `yaml:"startup_cleanup_connection,omitempty"`
	PingIntervalMs              int                             `yaml:"ping_interval_ms,omitempty"`
	MessageFormat               string                          `yaml:"message_format,omitempty"`
	AttachmentFilename          string                          `yaml:"attachment_filename,omitempty"`
	MessageSubject              string                          `yaml:"message_subject,omitempty"`
	MessageSubjectMode          config.MessageSubjectMode       `yaml:"message_subject_mode,omitempty"`
	MessageTo                   string                          `yaml:"message_to,omitempty"`
	SubjectClientID             *bool                           `yaml:"subject_client_id,omitempty"`
	EncryptionPassphrase        string                          `yaml:"encryption_passphrase,omitempty"`
	ClientEncryptionPassphrases map[byte]string                 `yaml:"client_encryption_passphrases,omitempty"`
	DNSServers                  []string                        `yaml:"dns_servers,omitempty"`
	PidFile                     string                          `yaml:"pid_file,omitempty"`
	GracefulShutdownMs          int                             `yaml:"graceful_shutdown_ms,omitempty"`
	BatchDelayMs                *int                            `yaml:"batch_delay_ms,omitempty"`
	BatchMaxFrames              int                             `yaml:"batch_max_frames,omitempty"`
	BatchMaxKB                  int                             `yaml:"batch_max_kb,omitempty"`
	BatchQueueSize              int                             `yaml:"batch_queue_size,omitempty"`
	AsyncDataSend               bool                            `yaml:"async_data_send,omitempty"`
	InboundQueueSize            int                             `yaml:"inbound_queue_size,omitempty"`
	InboundQueueWaitMs          int                             `yaml:"inbound_queue_wait_ms,omitempty"`
	ZeroRTTOpen                 *bool                           `yaml:"zero_rtt_open,omitempty"`
}

func compactYAMLConfig(cfg *config.Config) ([]byte, error) {
	doc := ssURLYAMLConfigDoc{
		Mode:     cfg.Mode,
		Listen:   cfg.Listen,
		Target:   cfg.Target,
		Accounts: make([]ssURLYAMLAccount, 0, len(cfg.Accounts)),
	}
	for _, account := range cfg.Accounts {
		out := ssURLYAMLAccount{
			Name:               account.Name,
			Host:               account.Host,
			Username:           account.Username,
			Password:           account.Password,
			OAuth2Token:        account.OAuth2Token,
			OAuth2TokenCommand: account.OAuth2TokenCommand,
			InsecureSkipVerify: account.InsecureSkipVerify,
			FolderSend:         account.FolderSend,
			FolderRecv:         account.FolderRecv,
		}
		if account.MessageFrom != "" && account.EffectiveMessageFrom() != config.DefaultMessageFrom {
			out.MessageFrom = account.EffectiveMessageFrom()
		}
		if account.TLS != "" && account.TLS != "implicit" {
			out.TLS = account.TLS
		}
		doc.Accounts = append(doc.Accounts, out)
	}
	if cfg.LogLevel != "" && cfg.LogLevel != "info" {
		doc.LogLevel = cfg.LogLevel
	}
	if cfg.ClientID != 0 {
		doc.ClientID = cfg.ClientID
	}
	if cfg.ClientVersion != "" {
		doc.ClientVersion = cfg.ClientVersion
	}
	if cfg.StatusAddr != "" {
		doc.StatusAddr = cfg.StatusAddr
	}
	if cfg.Reorder != nil && !cfg.ReorderEnabled() {
		v := false
		doc.Reorder = &v
	}
	if cfg.EffectiveMultipathMode() != config.MultipathModeStreamAffinity {
		doc.MultipathMode = cfg.EffectiveMultipathMode()
	}
	if cfg.OpenTimeoutSec != 0 && cfg.OpenTimeoutSec != 30 {
		doc.OpenTimeoutSec = cfg.OpenTimeoutSec
	}
	if cfg.DialTimeoutSec != 0 && cfg.DialTimeoutSec != 10 {
		doc.DialTimeoutSec = cfg.DialTimeoutSec
	}
	if cfg.ReconnectInitialDelayMs != 0 && cfg.ReconnectInitialDelayMs != 500 {
		doc.ReconnectInitialDelayMs = cfg.ReconnectInitialDelayMs
	}
	if cfg.ReconnectMaxDelayMs != 0 && cfg.ReconnectMaxDelayMs != 30000 {
		doc.ReconnectMaxDelayMs = cfg.ReconnectMaxDelayMs
	}
	if cfg.ReconnectBackoff != 0 && cfg.ReconnectBackoff != 1.5 {
		doc.ReconnectBackoff = cfg.ReconnectBackoff
	}
	if cfg.ThrottleBackoffMs != 0 {
		doc.ThrottleBackoffMs = cfg.ThrottleBackoffMs
	}
	if cfg.PollIntervalMs != 0 && cfg.PollIntervalMs != 3000 {
		doc.PollIntervalMs = cfg.PollIntervalMs
	}
	if cfg.ActivePollIntervalMs != 0 && cfg.ActivePollIntervalMs != 100 {
		doc.ActivePollIntervalMs = cfg.ActivePollIntervalMs
	}
	if cfg.ActivePollDurationMs != 0 && cfg.ActivePollDurationMs != 5000 {
		doc.ActivePollDurationMs = cfg.ActivePollDurationMs
	}
	if cfg.LazyExpungeThreshold_ != 0 && cfg.LazyExpungeThreshold_ != 16 {
		doc.LazyExpungeThreshold = cfg.LazyExpungeThreshold_
	}
	if cfg.LazyExpungeMaxAgeMs != 0 && cfg.LazyExpungeMaxAgeMs != 30000 {
		doc.LazyExpungeMaxAgeMs = cfg.LazyExpungeMaxAgeMs
	}
	if cfg.EffectiveStartupCleanupConnection() != config.StartupCleanupConnectionFallback {
		doc.StartupCleanupConnection = cfg.EffectiveStartupCleanupConnection()
	}
	if cfg.PingIntervalMs != 0 {
		doc.PingIntervalMs = cfg.PingIntervalMs
	}
	if cfg.EffectiveMessageFormat() != "attachment" {
		doc.MessageFormat = cfg.EffectiveMessageFormat()
	}
	if cfg.AttachmentFilename != "" && cfg.AttachmentFilename != "tunnel.bin" {
		doc.AttachmentFilename = cfg.AttachmentFilename
	}
	if cfg.MessageSubject != "" && cfg.EffectiveMessageSubject() != config.DefaultMessageSubject {
		doc.MessageSubject = cfg.EffectiveMessageSubject()
	}
	if cfg.EffectiveMessageSubjectMode() != config.MessageSubjectModeFixed {
		doc.MessageSubjectMode = cfg.EffectiveMessageSubjectMode()
	}
	if cfg.MessageTo != "" && cfg.EffectiveMessageTo() != config.DefaultMessageTo {
		doc.MessageTo = cfg.EffectiveMessageTo()
	}
	if cfg.SubjectClientID != nil && !cfg.SubjectClientIDEnabled() {
		v := false
		doc.SubjectClientID = &v
	}
	doc.EncryptionPassphrase = cfg.EncryptionPassphrase
	doc.ClientEncryptionPassphrases = cfg.ClientEncryptionPassphrases
	doc.DNSServers = cfg.DNSServers
	doc.PidFile = cfg.PidFile
	if cfg.GracefulShutdownMs != 0 && cfg.GracefulShutdownMs != 3000 {
		doc.GracefulShutdownMs = cfg.GracefulShutdownMs
	}
	if cfg.BatchDelayMsSet || cfg.BatchDelayMs != 0 {
		if cfg.BatchDelayMs != 2 {
			v := cfg.BatchDelayMs
			doc.BatchDelayMs = &v
		}
	}
	if cfg.BatchMaxFrames_ != 0 && cfg.BatchMaxFrames_ != 64 {
		doc.BatchMaxFrames = cfg.BatchMaxFrames_
	}
	if cfg.BatchMaxKB != 0 && cfg.BatchMaxKB != 256 {
		doc.BatchMaxKB = cfg.BatchMaxKB
	}
	if cfg.BatchQueueSize_ != 0 && cfg.BatchQueueSize_ != 256 {
		doc.BatchQueueSize = cfg.BatchQueueSize_
	}
	doc.AsyncDataSend = cfg.AsyncDataSend
	if cfg.InboundQueueSize_ != 0 && cfg.InboundQueueSize_ != 64 {
		doc.InboundQueueSize = cfg.InboundQueueSize_
	}
	if cfg.InboundQueueWaitMs != 0 && cfg.InboundQueueWaitMs != 30000 {
		doc.InboundQueueWaitMs = cfg.InboundQueueWaitMs
	}
	if cfg.ZeroRTTOpen != nil && cfg.ZeroRTTOpenEnabled() {
		v := true
		doc.ZeroRTTOpen = &v
	}
	out, err := yaml.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encode compact config for URL: %w", err)
	}
	return out, nil
}

type pluginOptionPair struct {
	key string
	val string
}

func ssURLQueryOptions(cfg *config.Config, skipDefaults bool) (string, error) {
	var pairs []pluginOptionPair
	add := func(key, val string) {
		if val == "" {
			return
		}
		pairs = append(pairs, pluginOptionPair{key: key, val: val})
	}
	addInt := func(key string, val int, def int) {
		if skipDefaults && val == def {
			return
		}
		if val == 0 && def == 0 {
			return
		}
		if val != 0 {
			add(key, strconv.Itoa(val))
		}
	}
	addBool := func(key string, val bool, def bool) {
		if skipDefaults && val == def {
			return
		}
		add(key, strconv.FormatBool(val))
	}
	addString := func(key, val, def string) {
		if val == "" || (skipDefaults && val == def) {
			return
		}
		add(key, val)
	}

	addString("mode", string(cfg.Mode), string(config.ModeClient))
	addString("log_level", cfg.LogLevel, "info")
	addInt("client_id", int(cfg.ClientID), 0)
	addString("client_version", cfg.ClientVersion, "")
	addString("status_addr", cfg.StatusAddr, "")
	if !skipDefaults || !cfg.ReorderEnabled() {
		addBool("reorder", cfg.ReorderEnabled(), true)
	}
	if cfg.EffectiveMultipathMode() != config.MultipathModeStreamAffinity || !skipDefaults {
		addString("multipath_mode", string(cfg.EffectiveMultipathMode()), string(config.MultipathModeStreamAffinity))
	}
	addInt("open_timeout_sec", cfg.OpenTimeoutSec, 30)
	addInt("dial_timeout_sec", cfg.DialTimeoutSec, 10)
	addInt("reconnect_initial_delay_ms", cfg.ReconnectInitialDelayMs, 500)
	addInt("reconnect_max_delay_ms", cfg.ReconnectMaxDelayMs, 30000)
	if cfg.ReconnectBackoff != 0 && (!skipDefaults || cfg.ReconnectBackoff != 1.5) {
		add("reconnect_backoff", strconv.FormatFloat(cfg.ReconnectBackoff, 'f', -1, 64))
	}
	addInt("throttle_backoff_ms", cfg.ThrottleBackoffMs, 0)
	addInt("poll_interval_ms", cfg.PollIntervalMs, 3000)
	addInt("active_poll_interval_ms", cfg.ActivePollIntervalMs, 100)
	addInt("active_poll_duration_ms", cfg.ActivePollDurationMs, 5000)
	addInt("lazy_expunge_threshold", cfg.LazyExpungeThreshold_, 16)
	addInt("lazy_expunge_max_age_ms", cfg.LazyExpungeMaxAgeMs, 30000)
	if cfg.EffectiveStartupCleanupConnection() != config.StartupCleanupConnectionFallback || !skipDefaults {
		addString("startup_cleanup_connection", string(cfg.EffectiveStartupCleanupConnection()),
			string(config.StartupCleanupConnectionFallback))
	}
	addInt("ping_interval_ms", cfg.PingIntervalMs, 0)
	addString("message_format", cfg.EffectiveMessageFormat(), "attachment")
	addString("attachment_filename", cfg.AttachmentFilename, "tunnel.bin")
	addString("message_subject", cfg.MessageSubject, config.DefaultMessageSubject)
	addString("message_subject_mode", string(cfg.EffectiveMessageSubjectMode()), string(config.MessageSubjectModeFixed))
	addString("message_to", cfg.MessageTo, config.DefaultMessageTo)
	if !skipDefaults || !cfg.SubjectClientIDEnabled() {
		addBool("subject_client_id", cfg.SubjectClientIDEnabled(), true)
	}
	addString("encryption_passphrase", cfg.EncryptionPassphrase, "")
	addString("client_encryption_passphrases", formatClientEncryptionPassphrases(cfg.ClientEncryptionPassphrases), "")
	if len(cfg.DNSServers) == 1 {
		add("dns_server", cfg.DNSServers[0])
	} else if len(cfg.DNSServers) > 1 {
		add("dns_servers", strings.Join(cfg.DNSServers, ","))
	}
	addString("pid_file", cfg.PidFile, "")
	addInt("graceful_shutdown_ms", cfg.GracefulShutdownMs, 3000)
	batchDelayMs := cfg.BatchDelayMs
	if !cfg.BatchDelayMsSet && batchDelayMs <= 0 {
		batchDelayMs = 2
	}
	if cfg.BatchDelayMsSet || cfg.BatchDelayMs != 0 || !skipDefaults {
		if !skipDefaults || batchDelayMs != 2 {
			add("batch_delay_ms", strconv.Itoa(batchDelayMs))
		}
	}
	addInt("batch_max_frames", cfg.BatchMaxFrames_, 64)
	addInt("batch_max_kb", cfg.BatchMaxKB, 256)
	addInt("batch_queue_size", cfg.BatchQueueSize_, 256)
	// SIP003 inline configs historically default async_data_send to true, so
	// include false even with skip-defaults to preserve normal YAML semantics.
	if cfg.AsyncDataSend || !skipDefaults {
		addBool("async_data_send", cfg.AsyncDataSend, true)
	} else {
		add("async_data_send", "false")
	}
	addInt("inbound_queue_size", cfg.InboundQueueSize_, 64)
	addInt("inbound_queue_wait_ms", cfg.InboundQueueWaitMs, 30000)
	if cfg.ZeroRTTOpen != nil {
		addBool("zero_rtt_open", cfg.ZeroRTTOpenEnabled(), false)
	} else if !skipDefaults {
		addBool("zero_rtt_open", false, false)
	}
	for i, account := range cfg.Accounts {
		addAccountOption := func(key, val, def string) {
			if val == "" || (skipDefaults && val == def) {
				return
			}
			add(suffixedAccountKey(key, i), val)
		}
		addAccountOption("name", account.Name, "")
		addAccountOption("imap_host", account.Host, "")
		addAccountOption("imap_username", account.Username, "")
		addAccountOption("imap_password", account.Password, "")
		addAccountOption("oauth2_token", account.OAuth2Token, "")
		addAccountOption("oauth2_token_command", account.OAuth2TokenCommand, "")
		addAccountOption("imap_tls", account.TLS, "implicit")
		if account.InsecureSkipVerify || !skipDefaults {
			add(suffixedAccountKey("imap_insecure_skip_verify", i), strconv.FormatBool(account.InsecureSkipVerify))
		}
		addAccountOption("folder_send", account.FolderSend, "")
		addAccountOption("folder_recv", account.FolderRecv, "")
		addAccountOption("message_from", account.MessageFrom, config.DefaultMessageFrom)
	}
	parts := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		parts = append(parts, escapePluginOption(pair.key)+"="+escapePluginOption(pair.val))
	}
	return strings.Join(parts, ";"), nil
}

func suffixedAccountKey(key string, idx int) string {
	if idx == 0 {
		return key
	}
	return fmt.Sprintf("%s_%d", key, idx+1)
}

func escapePluginOption(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '\\', '=', ';':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func ssURLUserinfo(method, password string) *url.Userinfo {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(method + ":" + password))
	return url.User(encoded)
}

func parsePluginOptions(s string) map[string]string {
	out := map[string]string{}
	var key, val strings.Builder
	inKey := true
	escaped := false
	flush := func() {
		if key.Len() > 0 {
			out[key.String()] = val.String()
		}
		key.Reset()
		val.Reset()
		inKey = true
	}
	for _, r := range s {
		if escaped {
			if inKey {
				key.WriteRune(r)
			} else {
				val.WriteRune(r)
			}
			escaped = false
			continue
		}
		switch r {
		case '\\':
			escaped = true
		case '=':
			if inKey {
				inKey = false
			} else {
				val.WriteRune(r)
			}
		case ';':
			flush()
		default:
			if inKey {
				key.WriteRune(r)
			} else {
				val.WriteRune(r)
			}
		}
	}
	flush()
	return out
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func optionString(opts map[string]string, key, def string) string {
	if v := opts[key]; v != "" {
		return v
	}
	return def
}

func optionInt(opts map[string]string, key string, def int) int {
	if v := opts[key]; v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func optionFloat(opts map[string]string, key string, def float64) float64 {
	if v := opts[key]; v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return def
}

func optionUint8(opts map[string]string, key string, def uint8) uint8 {
	if v := opts[key]; v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 255 {
			return uint8(n)
		}
	}
	return def
}

func optionBool(opts map[string]string, key string, def bool) bool {
	if v := opts[key]; v != "" {
		switch strings.ToLower(v) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return def
}

func parseClientEncryptionPassphrases(raw string) (map[byte]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	result := map[byte]string{}
	for _, part := range strings.Split(raw, ",") {
		idText, passphrase, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return nil, fmt.Errorf("invalid client_encryption_passphrases entry %q (expected id:passphrase)", part)
		}
		id, err := strconv.Atoi(strings.TrimSpace(idText))
		if err != nil || id <= 0 || id > 255 {
			return nil, fmt.Errorf("invalid client_encryption_passphrases client ID %q (expected 1-255)", idText)
		}
		if strings.TrimSpace(passphrase) == "" {
			return nil, fmt.Errorf("invalid client_encryption_passphrases[%d]: passphrase must not be empty", id)
		}
		result[byte(id)] = passphrase
	}
	return result, nil
}

func formatClientEncryptionPassphrases(passphrases map[byte]string) string {
	if len(passphrases) == 0 {
		return ""
	}
	ids := make([]int, 0, len(passphrases))
	for id := range passphrases {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%d:%s", id, passphrases[byte(id)]))
	}
	return strings.Join(parts, ",")
}

type dnsLookupFunc func(context.Context, string) ([]string, error)

// applyDNSOverride installs a process-wide net.DefaultResolver when
// dns_servers is configured, or when the local resolver cannot resolve
// any configured tunnel hostname and no explicit DNS was provided.
func applyDNSOverride(cfg *config.Config) error {
	servers, reason, err := dnsOverrideServers(cfg, net.DefaultResolver.LookupHost)
	if err != nil {
		return err
	}
	if len(servers) == 0 {
		return nil
	}

	tlog.Infof("dns: fallback resolvers %v (%s)", servers, reason)
	installDNSResolver(servers)
	return nil
}

func dnsOverrideServers(cfg *config.Config, lookup dnsLookupFunc) ([]string, string, error) {
	servers := make([]string, 0, len(cfg.DNSServers))
	for _, s := range cfg.DNSServers {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(s); err != nil {
			if strings.Contains(s, ":") && !strings.HasPrefix(s, "[") {
				s = "[" + s + "]:53"
			} else {
				s = s + ":53"
			}
		}
		servers = append(servers, s)
	}
	if len(cfg.DNSServers) > 0 && len(servers) == 0 {
		return nil, "", fmt.Errorf("dns_servers configured but all entries were empty")
	}
	if len(servers) > 0 {
		return servers, "configured", nil
	}

	probeHost := firstDNSProbeHost(cfg)
	if probeHost == "" {
		return nil, "", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := lookup(ctx, probeHost); err == nil {
		return nil, "", nil
	} else {
		var dnsErr *net.DNSError
		reason := fmt.Sprintf("local resolver failed for %q: %v", probeHost, err)
		if errors.As(err, &dnsErr) && dnsErr.Name != "" {
			reason = fmt.Sprintf("local resolver failed for %q: %s", dnsErr.Name, dnsErr.Err)
		}
		return []string{defaultDNSFallbackServer}, reason, nil
	}
}

func firstDNSProbeHost(cfg *config.Config) string {
	for _, acc := range cfg.Accounts {
		if host := dnsProbeHost(acc.Host); host != "" {
			return host
		}
	}
	if cfg.Mode == config.ModeServer {
		return dnsProbeHost(cfg.Target)
	}
	return ""
}

func dnsProbeHost(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if host == "" || net.ParseIP(host) != nil {
		return ""
	}
	return host
}

func installDNSResolver(servers []string) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	dialer = netprotect.WrapDialer(dialer)
	var rrIndex uint32
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			i := int(atomic.AddUint32(&rrIndex, 1)-1) % len(servers)
			// Force UDP — some Android networks block outbound TCP/53.
			return dialer.DialContext(ctx, "udp", servers[i])
		},
	}
}

// killStalePid reads the PID from path, checks whether that process is
// still alive, and kills it. Best-effort — any error is logged and
// swallowed so startup proceeds regardless.
func killStalePid(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return // no file or unreadable — nothing to kill
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return
	}
	// On Unix, FindProcess always succeeds; Signal(0) probes liveness.
	if proc.Signal(syscall.Signal(0)) != nil {
		return // process no longer exists
	}
	tlog.Warnf("killing stale tunnel process pid=%d", pid)
	_ = proc.Signal(syscall.SIGTERM)
	// Give it a moment to release the port.
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		if proc.Signal(syscall.Signal(0)) != nil {
			tlog.Infof("stale process pid=%d exited", pid)
			return
		}
	}
	// Still alive — force-kill.
	_ = proc.Kill()
	time.Sleep(100 * time.Millisecond)
	tlog.Warnf("force-killed stale process pid=%d", pid)
}

// writePidFile writes the current PID to path.
func writePidFile(path string) {
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())+"\n"), 0644); err != nil {
		tlog.Warnf("failed to write pid file %s: %v", path, err)
	}
}
