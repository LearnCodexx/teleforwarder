# TeleForwarder

`teleforwarder` is a lightweight, concurrent, and robust Go package designed to continuously consume structured error payloads from a Redis List queue and forward them as beautifully formatted HTML alerts to a designated Telegram chat or group.

It features built-in **graceful shutdown** capabilities, prevents goroutine leaks via synchronization mechanisms, and optimizes network resource utilization using a reusable HTTP client.


## 🛠 Features

* **Synchronous Queue Consumption:** Utilizes non-infinite Redis `BLPop` blocking operations to safely listen for incoming events while respecting context deadlines.
* **Asynchronous Dispatched Alerts:** Processes and delivers Telegram alerts concurrently without blocking the main event-loop consumption.
* **Graceful Shutdown Support:** Leverages `sync.WaitGroup` and Go `context.Context` to guarantee that in-flight Telegram HTTP requests finish processing completely before the application stops.
* **Beautiful HTML Layouts:** Dynamically formats Telegram alerts with customized emojis and typography depending on the log's severity layer (`critical`, `error`/`danger`, `info`, `warning`).

## 📋 Prerequisites

* Go `1.21` or higher.
* A running Redis instance.
* A Telegram Bot Token (generated via `@BotFather`).
* A target Telegram Chat/Group ID (Make sure the bot has been added to the group as an Administrator).


## 📦 Installation

To import this package into your project, run:

```bash
go get [github.com/learncodexx/teleforwarder](https://github.com/learncodexx/teleforwarder)

```

## 🚀 Usage Guide

### 1. Expected JSON Payload Structure

The worker expects messages in the Redis List queue to be marshaled JSON objects matching the following format:

```json
{
  "timestamp": "2026-06-11 15:04:05",
  "environment": "production",
  "service": "payment-service",
  "severity": "critical",
  "error_type": "DatabaseConnectionTimeout",
  "description": "Failed to acquire connection from pool after 5000ms.",
  "raw_error": "driver: bad connection",
  "file": "repository/user_repository.go",
  "line": 42,
  "function": "FindUserByID"
}

```

### 2. How to Push Alerts to the Queue (Producer Side)

For other developers working on different services, here is a code snippet demonstrating how to format and push an error log into the Redis queue so that your `teleforwarder` worker can pick it up:

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"[github.com/redis/go-redis/v9](https://github.com/redis/go-redis/v9)"
	"[github.com/learncodexx/teleforwarder](https://github.com/learncodexx/teleforwarder)"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()

	// Create the structured log
	errPayload := teleforwarder.CustomError{
		Timestamp:   time.Now().Format("2006-01-02 15:04:05"),
		Environment: "production",
		Service:     "auth-service",
		Severity:    "error",
		ErrorType:   "TokenValidationError",
		Description: "Expired or malformed JWT token detected during gateway validation.",
		RawError:    "token contains an invalid number of segments",
		File:        "middleware/auth.go",
		Line:        28,
		Function:    "ValidateToken",
	}

	// Marshal to JSON string
	jsonData, err := json.Marshal(errPayload)
	if err != nil {
		log.Fatalf("Failed to marshal error: %v", err)
	}

	// Push to the designated Redis List queue
	queueName := "service-alerts"
	err = rdb.RPush(ctx, queueName, jsonData).Err()
	if err != nil {
		log.Printf("Failed to push alert to Redis: %v", err)
	} else {
		log.Println("✅ Error log successfully pushed to Redis queue!")
	}
}

```

### 3. Implementing the Worker (Consumer Side)

Here is how you can correctly set up a separate `main.go` file dedicated to running the worker background process. This includes tracking OS system signals for graceful service terminations (e.g., inside Docker/Kubernetes):

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"[github.com/redis/go-redis/v9](https://github.com/redis/go-redis/v9)"
	"[github.com/learncodexx/teleforwarder](https://github.com/learncodexx/teleforwarder)"
)

func main() {
	// 1. Initialize your Redis Driver connection
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// 2. Create a cancelable context to handle lifecycle triggers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Define the TeleForwarder configuration credentials
	cfg := teleforwarder.Config{
		RedisClient:  rdb,
		QueueName:    "service-alerts",
		BotToken:     "123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ",
		TargetChatID: "-100123456789", // Always include the minus symbol for group chats
	}

	// 4. Capture OS signals asynchronously to coordinate graceful shutdowns
	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
		<-stop // Block here until SIGINT or SIGTERM is intercepted
		
		log.Println("🔄 OS Signal received! Shutting down TeleForwarder worker cleanly...")
		cancel() // Triggers ctx.Done() inside the worker loop
	}()

	log.Println("🚀 TeleForwarder Application Service Started successfully.")
	
	// 5. Run the worker process in a blocking fashion on the main thread
	teleforwarder.StartWorker(ctx, cfg)

	// Buffer sleep to ensure absolute clearing of terminal standard outputs
	time.Sleep(200 * time.Millisecond)
	log.Println("🛑 Application fully stopped.")
}

```

---

## ⚙️ Architecture & Internal Mechanics

Understanding how `StartWorker` treats concurrency is vital to avoiding system resource drainage:

1. **Loop & Select:** The worker enters a persistent loop. It attempts a `BLPop` operation on Redis with a **5-second timeout constraint**.
2. **Timeout Safety:** Rather than blocking indefinitely (`0`), the 5-second interval allows the thread to safely break and inspect `case <-ctx.Done():` during quiet queue windows.
3. **WaitGroup Safeguards:** Every single JSON unmarshalling and outgoing Telegram API network task runs inside its own isolated goroutine. A `sync.WaitGroup` counter tracks these states. When `ctx.Done()` fires, the application delays its exit sequence until `wg.Wait()` confirms all ongoing HTTP requests have concluded cleanly.

---

## 🎨 Visual Preview

When an alert is pushed to your Telegram Group, it formats into an explicit semantic template layout:

🔥 **❌ ERROR DETECTED**
`----------------------------------------`
**🌐 Environment:** #production
**🏗️ Service:** `payment-service`
**🏷️ Error Type:** `DatabaseConnectionTimeout`
`----------------------------------------`

**📝 Description:**

> Failed to acquire connection from pool after 5000ms.

**📍 Location:**
`repository/user_repository.go:42`
▶️ *func FindUserByID()*

**💥 Raw Error Detail:**

```go
driver: bad connection

```

⏰ *Reported at: 2026-06-11 15:04:05*

---

## ❓ Troubleshooting & FAQ

**Q: Why are my messages getting stuck in Redis and not forwarding?**

* **A:** Verify that the `QueueName` string assigned to `Config` matches exactly with the key used by the publisher when calling `RPush`.

**Q: The worker returns `telegram API returned status 400`. What's wrong?**

* **A:** This usually means your `TargetChatID` or `BotToken` is incorrect. If you are sending alerts to a Telegram Group/Channel, make sure the ID starts with a minus sign (e.g., `-100xxxxxxxxx`) and that your bot has been invited to that chat room.

**Q: Can I run multiple instances of this worker safely?**

* **A:** Yes! Because it uses Redis `BLPop`, messages are safely atomic. If you run multiple containers of this worker, Redis will naturally balance the load, distributing one log entry to one single worker.

---

## 📄 License

This package is distributed under the standard MIT License. Feel free to modify and expand its behavior.

```

```
