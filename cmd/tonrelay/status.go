package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"

	"github.com/TONresistor/tonrelay/internal/config"
	"github.com/TONresistor/tonrelay/internal/monitor"
	"github.com/TONresistor/tonrelay/internal/service"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show relay status",
	RunE: func(cmd *cobra.Command, args []string) error {
		live, _ := cmd.Flags().GetBool("live")
		cfg, _ := config.Load(cfgPath)

		if jsonOut {
			return printStatusJSON()
		}

		if live {
			m := monitor.NewDashboardModel(cfg, Version)
			p := tea.NewProgram(m, tea.WithAltScreen())
			_, err := p.Run()
			return err
		}

		return printStatusSnapshot()
	},
}

func printStatusSnapshot() error {
	svc, _ := service.GetStatus()

	// State
	if svc != nil && svc.Active {
		fmt.Print("state:    running")
		if svc.ActiveTime != "" {
			if t, err := time.Parse("Mon 2006-01-02 15:04:05 MST", svc.ActiveTime); err == nil {
				up := time.Since(t).Truncate(time.Second)
				fmt.Printf("  (up %s)", formatUptime(up))
			}
		}
		fmt.Println()
	} else {
		fmt.Println("state:    stopped")
	}

	// Config info
	if c, err := config.Load(cfgPath); err == nil {
		fmt.Printf("endpoint: %s:%s\n", c.ExternalIP, extractPort(c.TunnelListenAddr))
		fmt.Printf("adnl:     %s\n", truncateID(config.GetADNLID(c)))

		if c.PaymentsEnabled {
			fmt.Printf("mode:     paid (%d nano/pkt)\n", c.Payments.MinPricePerPacketRoute)
		} else {
			fmt.Println("mode:     free")
		}
	}

	// Quick metrics scrape
	mon := monitor.New()
	mon.Start()
	time.Sleep(500 * time.Millisecond)
	m := mon.GetMetrics()
	mon.Stop()

	switch {
	case m.DHTPublished:
		fmt.Println("dht:      published")
	case strings.HasPrefix(m.LastError, "no journal access"):
		fmt.Println("dht:      unknown (no journal access — run with sudo)")
	default:
		fmt.Println("dht:      waiting")
	}

	if m.PacketsRouted > 0 || m.PacketsSent > 0 || m.PacketsRecv > 0 {
		fmt.Printf("packets:  %s routed, %s in, %s out\n",
			monitor.FormatPackets(m.PacketsRouted),
			monitor.FormatPackets(m.PacketsRecv),
			monitor.FormatPackets(m.PacketsSent))
	}

	if m.InboundSect > 0 {
		fmt.Printf("inbound:  %d sections\n", m.InboundSect)
	}

	return nil
}

func printStatusJSON() error {
	svcStatus, _ := service.GetStatus()

	out := map[string]interface{}{
		"service_active":  svcStatus != nil && svcStatus.Active,
		"service_enabled": svcStatus != nil && svcStatus.Enabled,
	}

	if svcStatus != nil {
		out["sub_state"] = svcStatus.SubState
		out["main_pid"] = svcStatus.MainPID
	}

	if c, err := config.Load(cfgPath); err == nil {
		out["external_ip"] = c.ExternalIP
		out["listen_addr"] = c.TunnelListenAddr
		out["adnl_id"] = config.GetADNLID(c)
		out["payments_enabled"] = c.PaymentsEnabled
		if c.PaymentsEnabled {
			out["price_route"] = c.Payments.MinPricePerPacketRoute
			out["price_out"] = c.Payments.MinPricePerPacketInOut
		}
	}

	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func formatUptime(d time.Duration) string {
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	mins := int(d.Minutes()) % 60

	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm", days, hours, mins)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, mins)
	}
	return fmt.Sprintf("%dm", mins)
}

func init() {
	statusCmd.Flags().Bool("live", false, "open live dashboard")
	rootCmd.AddCommand(statusCmd)
}
