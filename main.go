package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/gin-gonic/gin"
	"atone-hands-on/producer"
	"atone-hands-on/producer/commands"
	"atone-hands-on/producer/events"
	"github.com/google/uuid"
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
)

// WebSocketアップグレードのためのアップグレーダー
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // すべてのオリジンからの接続を許可
	},
}

// WebSocket接続を管理するクライアント構造体
type Client struct {
	conn *websocket.Conn
	send chan []byte
}

const (
	region   = "ap-northeast-1"
	dbSource = "root:password@tcp(127.0.0.1:3306)/atone_hands_on?parseTime=true&charset=utf8mb4"
)

func main() {
	// Kinesisクライアントの初期化
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	kinesisClient := kinesis.NewFromConfig(cfg)

	// DB接続の初期化
	db, err := sql.Open("mysql", dbSource)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	// クライアントを管理するためのハブ
	clients := make(map[*Client]bool)
	broadcast := make(chan []byte)

	// バックグラウンドでブロードキャストを処理
	go func() {
		for {
			msg := <-broadcast
			for client := range clients {
				if err := client.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
					log.Printf("WebSocket write error: %v", err)
					client.conn.Close()
					delete(clients, client)
				}
			}
		}
	}()


	r := gin.Default()
	r.LoadHTMLGlob("templates/*")

	// 商品購入コマンドを受け付けるエンドポイント
	r.POST("/purchase", func(c *gin.Context) {
		var cmd commands.PurchaseCommand
		if err := c.ShouldBindJSON(&cmd); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// 購入完了イベントを生成
		purchaseEvent := events.PurchaseCompletedEvent{
			EventType: "PurchaseCompletedEvent",
			OrderID:   "order-" + cmd.UserID, // シンプルなID生成
			UserID:    cmd.UserID,
			Amount:    cmd.Amount,
		}

		// Kinesisにイベントを発行
		err := producer.EmitEvent(ctx, kinesisClient, purchaseEvent)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to emit event"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "Purchase command processed and event emitted"})
	})

	// すぐ払いモードの会員請求発行エンドポイント
	r.POST("/create-bill/immediate", func(c *gin.Context) {
		var cmd commands.CreateMemberBillCommand
		if err := c.ShouldBindJSON(&cmd); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		billEvent := events.MemberBillCreatedEvent{
			EventType:  "MemberBillCreatedEvent",
			BillID:     uuid.New().String(),
			PromiseID:  cmd.PromiseID,
			UserID:     cmd.UserID,
			Amount:     cmd.Amount,
			IssuedDate: time.Now().Format("2006-01-02"),
		}

		err := producer.EmitEvent(ctx, kinesisClient, billEvent)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to emit event"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "Member bill created event emitted for immediate payment"})
	})

	// 支払い完了Webhookを受け付けるエンドポイント
	r.POST("/webhook/payment-completed", func(c *gin.Context) {
		var webhookPayload struct {
			BillID string `json:"bill_id"`
			UserID string `json:"user_id"`
			Amount int    `json:"amount"`
		}
		if err := c.ShouldBindJSON(&webhookPayload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		paymentEvent := events.PaymentCompletedEvent{
			EventType: "PaymentCompletedEvent",
			BillID:    webhookPayload.BillID,
			UserID:    webhookPayload.UserID,
			Amount:    webhookPayload.Amount,
			PaidDate:  time.Now().Format("2006-01-02"),
		}

		err := producer.EmitEvent(ctx, kinesisClient, paymentEvent)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to emit event"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "Payment completed event emitted"})
	})

	// ユーザーの購入履歴と請求状況を取得するエンドポイント
	r.GET("/user/:userId/status", func(c *gin.Context) {
		userId := c.Param("userId")

		// payment_promisesテーブルから購入履歴を取得
		promises, err := getPaymentPromises(db, userId)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get payment promises"})
			return
		}

		// member_billsテーブルから請求状況を取得
		bills, err := getMemberBills(db, userId)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get member bills"})
			return
		}

		c.HTML(http.StatusOK, "status.html", gin.H{
			"userId":          userId,
			"purchaseHistory": promises,
			"billingStatus":   bills,
		})
	})

	// WebSocketエンドポイント
	r.GET("/ws", func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("WebSocket upgrade error: %v", err)
			return
		}

		client := &Client{conn: conn, send: make(chan []byte, 256)}
		clients[client] = true
		defer func() {
			conn.Close()
			delete(clients, client)
		}()

		// クライアントからのメッセージを処理（ここでは何もしない）
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				break
			}
		}
	})

	r.Run(":8080")
}

// getPaymentPromisesは指定されたユーザーの支払約束を取得する
func getPaymentPromises(db *sql.DB, userId string) ([]gin.H, error) {
	rows, err := db.Query("SELECT order_id, amount, due_date, payment_mode, created_at FROM payment_promises WHERE user_id = ?", userId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var promises []gin.H
	for rows.Next() {
		var orderId, paymentMode string
		var amount int
		var dueDate, createdAt time.Time
		if err := rows.Scan(&orderId, &amount, &dueDate, &paymentMode, &createdAt); err != nil {
			return nil, err
		}
		promises = append(promises, gin.H{
			"orderId":     orderId,
			"amount":      amount,
			"dueDate":     dueDate.Format("2006-01-02"),
			"paymentMode": paymentMode,
			"createdAt":   createdAt,
		})
	}
	return promises, nil
}

// getMemberBillsは指定されたユーザーの請求状況を取得する
func getMemberBills(db *sql.DB, userId string) ([]gin.H, error) {
	rows, err := db.Query("SELECT id, amount, status, issued_date, paid_date FROM member_bills WHERE user_id = ?", userId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var bills []gin.H
	for rows.Next() {
		var id, status string
		var amount int
		var issuedDate time.Time
		var paidDate sql.NullTime // paid_dateはNULLの可能性があるためsql.NullTimeを使用
		if err := rows.Scan(&id, &amount, &status, &issuedDate, &paidDate); err != nil {
			return nil, err
		}

		bill := gin.H{
			"billId":      id,
			"amount":      amount,
			"status":      status,
			"issuedDate":  issuedDate.Format("2006-01-02"),
			"paidDate":    nil,
		}
		if paidDate.Valid {
			bill["paidDate"] = paidDate.Time.Format("2006-01-02")
		}
		bills = append(bills, bill)
	}
	return bills, nil
}