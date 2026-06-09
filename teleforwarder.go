package teleforwarder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
//	func main() {
//	    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//
//	    cfg := teleforwarder.Config{
//	        RedisClient:  rdb,
//	        QueueName:    "service-alerts",
//	        BotToken:     "123456:ABC-DEF",
//	        TargetChatID: "-100123456789",
//	    }
//
//	    // This call is blocking; run inside a goroutine if needed
//	    teleforwarder.StartWorker(context.Background(), cfg)
//	}
func StartWorker(ctx context.Context, cfg Config) {
	log.Println("🤖 TeleForwarder Worker is running, listening to queue:", cfg.QueueName)

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping TeleForwarder worker...")
			return
		default:
			// Membaca data dari Redis List (Blocking Pop)
			results, err := cfg.RedisClient.BLPop(ctx, 0, cfg.QueueName).Result()
			if err != nil {
				log.Printf("[TeleForwarder] Error reading from Redis: %v. Retrying in 2s...", err)
				time.Sleep(2 * time.Second)
				continue
			}

			jsonPayload := results[1]

			// Proses pengiriman ke Telegram menggunakan Goroutine agar tidak blocking antrean selanjutnya
			go func(payload string) {
				var customErr CustomError
				if err := json.Unmarshal([]byte(payload), &customErr); err != nil {
					log.Printf("[TeleForwarder] Failed to unmarshal log: %v", err)
					return
				}

				if err := sendTelegramAlert(cfg, customErr); err != nil {
					log.Printf("[TeleForwarder] Failed to send Telegram alert: %v", err)
				}
			}(jsonPayload)
		}
	}
}

func sendTelegramAlert(cfg Config, errReport CustomError) error {
	emoji := "⚠️"
	switch errReport.Severity {
	case "critical":
		emoji = "🚨"
	case "danger":
		emoji = "🔥"
	case "info":
		emoji = "ℹ️"
	}

	message := fmt.Sprintf(
		"%s *\\[%s\\] %s*\n"+
			"*Severity:* `%s` | *Type:* `%s`\n\n"+
			"*Description:*\n%s\n\n"+
			"*Location:* `%s:%d`\n"+
			"*Function:* `%s`\n"+
			"*Raw Error:* `%s`\n\n"+
			"⏰ _%s_",
		emoji, errReport.Environment, errReport.Service,
		errReport.Severity, errReport.ErrorType,
		errReport.Description,
		errReport.File, errReport.Line,
		errReport.Function,
		errReport.RawError,
		errReport.Timestamp,
	)

	telegramPayload := map[string]string{
		"chat_id":    cfg.TargetChatID,
		"text":       message,
		"parse_mode": "Markdown",
	}

	jsonValue, err := json.Marshal(telegramPayload)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", cfg.BotToken)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram API returned status %d", resp.StatusCode)
	}

	log.Printf("[TeleForwarder] Forwarded alert [%s] to Telegram", errReport.ErrorType)
	return nil
}
