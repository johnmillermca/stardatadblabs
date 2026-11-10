// invite-mailer — sends StarData invite emails via SMTP
// POST /send-invite   {"to":"x@y.com","name":"Ana Torres","levels":["doris.analyst"],"mfa":false}
// GET  /healthz       → 200 ok
//
// Configuration (env vars):
//   SMTP_HOST  — SMTP server hostname (default: smtp.gmail.com)
//   SMTP_PORT  — SMTP server port    (default: 587)
//   SMTP_USER  — SMTP login username (also used as MAIL FROM address)
//   SMTP_PASS  — SMTP login password / app-password
//   FROM_NAME  — display name for the From header (default: StarData Platform)
//
// Legacy aliases (still accepted for backwards compatibility):
//   GMAIL_USER → SMTP_USER   GMAIL_PASS → SMTP_PASS
package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/smtp"
	"os"
	"strings"
	"time"
)

type inviteRequest struct {
	To     string   `json:"to"`
	Name   string   `json:"name"`
	Levels []string `json:"levels"`
	MFA    bool     `json:"mfa"`
}

var (
	smtpHost string
	smtpPort string
	smtpUser string
	smtpPass string
	fromName string
)

func main() {
	smtpHost = getEnv("SMTP_HOST", getEnv("GMAIL_HOST", "smtp.gmail.com"))
	smtpPort = getEnv("SMTP_PORT", "587")
	smtpUser = mustEnvAlias("SMTP_USER", "GMAIL_USER")
	smtpPass = mustEnvAlias("SMTP_PASS", "GMAIL_PASS")
	fromName = getEnv("FROM_NAME", "StarData Platform")

	mux := http.NewServeMux()
	mux.HandleFunc("/send-invite", handleSendInvite)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})

	log.Println("invite-mailer listening on :8025")
	if err := http.ListenAndServe(":8025", mux); err != nil {
		log.Fatal(err)
	}
}

func handleSendInvite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Allow the browser to call this directly (CORS for www.stardatadb.com)
	w.Header().Set("Access-Control-Allow-Origin", "https://www.stardatadb.com")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
	if err != nil {
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}

	var req inviteRequest
	if err := json.Unmarshal(body, &req); err != nil || req.To == "" || req.Name == "" {
		http.Error(w, "bad request: to and name are required", http.StatusBadRequest)
		return
	}

	if err := sendInviteEmail(req); err != nil {
		log.Printf("send error to %s: %v", req.To, err)
		http.Error(w, "failed to send email", http.StatusInternalServerError)
		return
	}

	log.Printf("invite sent to %s (%s) levels=%v", req.To, req.Name, req.Levels)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

func sendInviteEmail(req inviteRequest) error {
	from := fmt.Sprintf("%s <%s>", fromName, smtpUser)
	subject := fmt.Sprintf("You've been invited to StarData — %s", req.Name)

	// Build levels summary
	levelLines := ""
	if len(req.Levels) > 0 {
		levelLines = "\nYour starting access levels:\n"
		for _, l := range req.Levels {
			parts := strings.SplitN(l, ".", 2)
			if len(parts) == 2 {
				levelLines += fmt.Sprintf("  • %s: %s\n", parts[0], parts[1])
			}
		}
	}
	mfaNote := ""
	if req.MFA {
		mfaNote = "\nNote: MFA is required at your first sign-in.\n"
	}

	body := fmt.Sprintf(`Hi %s,

You've been invited to the StarData platform.

Click the link below to create your password or register a passkey.
This link is valid for 3 days.

  https://www.stardatadb.com
%s%s
If you weren't expecting this invite, you can safely ignore it.

— The StarData Platform Team
  https://www.stardatadb.com
`, req.Name, levelLines, mfaNote)

	msg := []byte(
		"From: " + from + "\r\n" +
			"To: " + req.To + "\r\n" +
			"Subject: " + subject + "\r\n" +
			"Date: " + time.Now().Format(time.RFC1123Z) + "\r\n" +
			"MIME-Version: 1.0\r\n" +
			"Content-Type: text/plain; charset=UTF-8\r\n" +
			"\r\n" +
			body,
	)

	// Connect to Gmail SMTP with STARTTLS
	auth := smtp.PlainAuth("", smtpUser, smtpPass, smtpHost)
	addr := smtpHost + ":" + smtpPort

	client, err := smtp.Dial(addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer client.Close()

	if err := client.StartTLS(&tls.Config{ServerName: smtpHost}); err != nil {
		return fmt.Errorf("starttls: %w", err)
	}
	if err := client.Auth(auth); err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	if err := client.Mail(smtpUser); err != nil {
		return fmt.Errorf("MAIL FROM: %w", err)
	}
	if err := client.Rcpt(req.To); err != nil {
		return fmt.Errorf("RCPT TO: %w", err)
	}
	wc, err := client.Data()
	if err != nil {
		return fmt.Errorf("DATA: %w", err)
	}
	if _, err := wc.Write(msg); err != nil {
		return fmt.Errorf("write body: %w", err)
	}
	return wc.Close()
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("required env var %s is not set", k)
	}
	return v
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// mustEnvAlias returns the value of primary env var k, falling back to alias
// if k is unset. Fatals if neither is set.
func mustEnvAlias(k, alias string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	if v := os.Getenv(alias); v != "" {
		return v
	}
	log.Fatalf("required env var %s (or legacy alias %s) is not set", k, alias)
	return ""
}
