package teleforwarder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds the necessary configuration and credentials required to run
// the teleforwarder worker background process.
type Config struct {
	RedisClient  *redis.Client
	QueueName    string
	BotToken     string
	TargetChatID string
}

// CustomError defines the structured log payload format generated
// by the reporter package.
type CustomError struct {
	Timestamp   string `json:"timestamp"`
	Environment string `json:"environment"`
	Service     string `json:"service"`
	Severity    string `json:"severity"`
	ErrorType   string `json:"error_type"`
	Description string `json:"description"`
	RawError    string `json:"raw_error"`
	File        string `json:"file"`
	Line        int    `json:"line"`
	Function    string `json:"function"`
}

// StartWorker spins up a synchronous, blocking background process that continuously
// monitors and consumes error logs from a designated Redis List queue.
//
// Every successfully consumed log payload is automatically unmarshalled and
// dispatched asynchronously to the specified Telegram chat/group destination.
//
// This function respects context cancellation for graceful shutdown sequences.
//
// Parameters:
//   - ctx: Controlling context used to gracefully stop the worker loop.
//   - cfg: Explicit initialization credentials containing Redis and Telegram tokens.
//
// Example:
//
//	 func main() {
//	     rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//
//	     cfg := teleforwarder.Config{
//	         RedisClient:  rdb,
//	         QueueName:    "service-alerts",
//	         BotToken:     "123456:ABC-DEF",
//	         TargetChatID: "-100123456789",
//	     }
//
//	     // This call is blocking; run inside a goroutine if needed
//	     teleforwarder.StartWorker(context.Background(), cfg)
//	 }
func StartWorker(ctx context.Context, cfg Config) {
	log.Println("🤖 TeleForwarder Worker is running, listening to queue:", cfg.QueueName)

	// Menggunakan WaitGroup untuk memastikan semua kiriman Telegram selesai sebelum worker benar-benar mati
	var wg sync.WaitGroup

	// Buat HTTP Client yang reusable agar menghemat resource network dan jandshake TCP
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("🔄 Context canceled, waiting for remaining Telegram alerts to finish sending...")
			wg.Wait() // Menunggu semua goroutine pengiriman Telegram selesai
			log.Println("🛑 TeleForwarder worker stopped cleanly.")
			return

		default:
			// Membaca data dari Redis List dengan blocking timeout 5 detik.
			// Timeout berkala memberi kesempatan bagi loop untuk mengecek case <-ctx.Done() saat antrean sepi.
			results, err := cfg.RedisClient.BLPop(ctx, 5*time.Second, cfg.QueueName).Result()
			if err != nil {
				// Jika error dikarenakan shutdown aplikasi (context canceled), langsung skip ke iterasi berikutnya
				if errors.Is(err, context.Canceled) || errors.Is(err, redis.Nil) {
					// redis.Nil artinya tidak ada data baru selama 5 detik, aman untuk lanjut loop
					continue
				}

				log.Printf("[TeleForwarder] Error reading from Redis: %v. Retrying in 2s...", err)

				// Sleep yang aman: jika ditengah-tengah sleep ada perintah shutdown, program bisa langsung merespon
				select {
				case <-ctx.Done():
					continue
				case <-time.After(2 * time.Second):
					continue
				}
			}

			if len(results) < 2 {
				continue
			}

			jsonPayload := results[1]

			// Tambah counter WaitGroup sebelum goroutine dijalankan
			wg.Add(1)
			go func(payload string) {
				defer wg.Done() // Kurangi counter setelah goroutine selesai diproses

				var customErr CustomError
				if err := json.Unmarshal([]byte(payload), &customErr); err != nil {
					log.Printf("[TeleForwarder] Failed to unmarshal log: %v", err)
					return
				}

				if err := sendTelegramAlert(httpClient, cfg, customErr); err != nil {
					log.Printf("[TeleForwarder] Failed to send Telegram alert: %v", err)
				}
			}(jsonPayload)
		}
	}
}

func sendTelegramAlert(client *http.Client, cfg Config, errReport CustomError) error {
	// 1. Indikator Warna/Emoji berdasarkan Tingkat Keparahan (Severity)
	emoji := "⚠️"
	headerColor := "🟡 WARNING"

	switch errReport.Severity {
	case "critical":
		emoji = "🚨"
		headerColor = "🔴 CRITICAL ALERT"
	case "danger", "error":
		emoji = "🔥"
		headerColor = "❌ ERROR DETECTED"
	case "info":
		emoji = "ℹ️"
		headerColor = "🔵 INFO LOG"
	}

	// 2. Desain Layout Pesan Menggunakan Fitur HTML Telegram
	// Menggunakan <pre><code> untuk blockquote kode agar mudah di-copy di HP
	message := fmt.Sprintf(
		"%s <b>%s</b>\n"+
			"----------------------------------------\n"+
			"<b>🌐 Environment:</b> #%s\n"+
			"<b>🏗️ Service:</b> <code>%s</code>\n"+
			"<b>🏷️ Error Type:</b> <code>%s</code>\n"+
			"----------------------------------------\n\n"+
			"<b>📝 Description:</b>\n"+
			"<blockquote>%s</blockquote>\n\n"+
			"<b>📍 Location:</b>\n"+
			"<code>%s:%d</code>\n"+
			"▶️ <i>func %s()</i>\n\n"+
			"<b>💥 Raw Error Detail:</b>\n"+
			"<pre><code class=\"language-go\">%s</code></pre>\n\n"+
			"⏰ <i>Reported at: %s</i>",
		emoji, headerColor,
		errReport.Environment,
		errReport.Service,
		errReport.ErrorType,
		errReport.Description,
		errReport.File, errReport.Line,
		errReport.Function,
		errReport.RawError,
		errReport.Timestamp,
	)

	telegramPayload := map[string]string{
		"chat_id":    cfg.TargetChatID,
		"text":       message,
		"parse_mode": "HTML",
	}

	jsonValue, err := json.Marshal(telegramPayload)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", cfg.BotToken)

	// Gunakan NewRequest dengan context bawaan http client agar mematuhi aturan timeout
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonValue))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram API returned status %d", resp.StatusCode)
	}

	log.Printf("[TeleForwarder] Beautiful alert [%s] sent to Telegram", errReport.ErrorType)
	return nil
}
