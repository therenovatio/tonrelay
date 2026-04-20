package monitor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultMetricsURL = "http://127.0.0.1:9091/metrics"

type Metrics struct {
	ServiceActive bool
	Uptime        time.Duration
	StartedAt     time.Time

	// From Prometheus metrics
	PacketsRouted float64
	PacketsSent   float64
	PacketsRecv   float64
	ActiveRoutes  int
	ActiveRoutesP int // paid
	InboundSect   int
	OutGateways   int

	// From log parsing
	DHTPublished bool
	LastError    string
	LastUpdate   time.Time
}

type Monitor struct {
	mu         sync.RWMutex
	metrics    Metrics
	cancel     func()
	done       chan struct{}
	stopOnce   sync.Once
	client     *http.Client
	metricsURL string
}

func New() *Monitor {
	return &Monitor{
		client:     &http.Client{Timeout: 2 * time.Second},
		metricsURL: defaultMetricsURL,
		done:       make(chan struct{}),
	}
}

func (m *Monitor) GetMetrics() Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.metrics
}

func (m *Monitor) Start() {
	// Only start the follower if we can actually read the unit's journal.
	// Without permission (not root / not in systemd-journal or adm), `journalctl -f`
	// hangs forever with no output and no error.
	if m.canReadJournal() {
		m.startLogWatcher()
	} else {
		m.mu.Lock()
		m.metrics.LastError = "no journal access — run with sudo"
		m.mu.Unlock()
	}

	// Scrape Prometheus metrics immediately + periodically
	go m.scrapeLoop()
}

func (m *Monitor) canReadJournal() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "journalctl", "-u", "tonrelay", "-n", "1", "--no-pager", "-o", "json").Output()
	if err != nil {
		return false
	}
	return len(bytes.TrimSpace(out)) > 0
}

func (m *Monitor) Stop() {
	m.stopOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
		}
		close(m.done)
	})
}

// scrapeLoop polls the Prometheus endpoint every 3 seconds
func (m *Monitor) scrapeLoop() {
	m.scrapeMetrics()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.scrapeMetrics()
		case <-m.done:
			return
		}
	}
}

func (m *Monitor) scrapeMetrics() {
	resp, err := m.client.Get(m.metricsURL)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Reset gauges before scrape so we get fresh values
	m.metrics.ActiveRoutes = 0
	m.metrics.ActiveRoutesP = 0
	m.metrics.InboundSect = 0
	m.metrics.OutGateways = 0

	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		m.parsePrometheusLine(line)
	}
	m.metrics.LastUpdate = time.Now()
}

func (m *Monitor) parsePrometheusLine(line string) {
	// Format: metric_name{labels} value
	// or:    metric_name value

	name, labels, val := parsePromLine(line)

	switch name {
	case "tunnel_packets_counter":
		typ := extractLabel(labels, "type")
		switch typ {
		case "routed":
			m.metrics.PacketsRouted = val
		case "in":
			m.metrics.PacketsRecv = val
		case "out":
			m.metrics.PacketsSent = val
		}
	case "tunnel_active_routes":
		paid := extractLabel(labels, "paid")
		if paid == "true" {
			m.metrics.ActiveRoutesP = int(val)
		} else {
			m.metrics.ActiveRoutes = int(val)
		}
	case "tunnel_active_inbound_sections":
		m.metrics.InboundSect = int(val)
	case "tunnel_active_out_gateways":
		m.metrics.OutGateways += int(val)
	}
}

func parsePromLine(line string) (name string, labels string, value float64) {
	// Split "metric{labels} value" or "metric value"
	braceStart := strings.IndexByte(line, '{')
	if braceStart >= 0 {
		name = line[:braceStart]
		braceEnd := strings.IndexByte(line[braceStart:], '}')
		if braceEnd >= 0 {
			labels = line[braceStart+1 : braceStart+braceEnd]
			valStr := strings.TrimSpace(line[braceStart+braceEnd+1:])
			value, _ = strconv.ParseFloat(valStr, 64)
		}
	} else {
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			name = parts[0]
			value, _ = strconv.ParseFloat(parts[1], 64)
		}
	}
	return
}

func extractLabel(labels, key string) string {
	for _, part := range strings.Split(labels, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 && kv[0] == key {
			return strings.Trim(kv[1], `"`)
		}
	}
	return ""
}

// Log watcher for DHT status, peer events, and errors

func (m *Monitor) startLogWatcher() {
	cmd := exec.Command("journalctl", "-u", "tonrelay", "-f", "-o", "json", "--no-pager", "--since", "10 min ago")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return
	}

	if err := cmd.Start(); err != nil {
		return
	}

	m.cancel = func() {
		cmd.Process.Kill()
		cmd.Wait()
	}

	go func() {
		defer cmd.Wait()
		scanner := bufio.NewScanner(stdout)
		buf := make([]byte, 0, 256*1024)
		scanner.Buffer(buf, 256*1024)

		for scanner.Scan() {
			m.parseLine(scanner.Text())
		}
	}()
}

type journalEntry struct {
	Message json.RawMessage `json:"MESSAGE"`
}

func (m *Monitor) parseLine(line string) {
	var entry journalEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		return
	}

	// MESSAGE can be a string or a byte array (when zerolog outputs ANSI codes)
	var msg string
	if err := json.Unmarshal(entry.Message, &msg); err != nil {
		// Try as byte array
		var bytes []byte
		if err := json.Unmarshal(entry.Message, &bytes); err != nil {
			return
		}
		msg = stripANSI(string(bytes))
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	switch {
	case strings.Contains(msg, "dht records updated") || strings.Contains(msg, "dht updated"):
		m.metrics.DHTPublished = true
	case strings.Contains(msg, "stored address in DHT") || strings.Contains(msg, "overlay nodes stored"):
		m.metrics.DHTPublished = true
	case strings.Contains(msg, "Tunnel started"):
		m.metrics.DHTPublished = false // will become true when DHT publish happens
	case strings.Contains(msg, "ERR") || strings.Contains(msg, "FTL"):
		if !strings.Contains(msg, "input failure") {
			m.metrics.LastError = truncate(msg, 120)
		}
	}
}

func stripANSI(s string) string {
	var out strings.Builder
	i := 0
	for i < len(s) {
		if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '[' {
			// Skip until 'm'
			j := i + 2
			for j < len(s) && s[j] != 'm' {
				j++
			}
			i = j + 1
			continue
		}
		out.WriteByte(s[i])
		i++
	}
	return out.String()
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

func FormatPackets(v float64) string {
	// Prometheus counter is in thousands (divided by 1000 in tunnel-node)
	total := v * 1000
	switch {
	case total >= 1_000_000_000:
		return fmt.Sprintf("%.2fB", total/1_000_000_000)
	case total >= 1_000_000:
		return fmt.Sprintf("%.2fM", total/1_000_000)
	case total >= 1_000:
		return fmt.Sprintf("%.1fK", total/1_000)
	default:
		return fmt.Sprintf("%.0f", total)
	}
}
