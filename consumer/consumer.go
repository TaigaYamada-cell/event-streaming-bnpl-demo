package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"atone-hands-on/consumer/events"
	"atone-hands-on/producer"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	streamName = "atone-events-stream"
	region     = "ap-northeast-1"
	dbSource   = "root:password@tcp(127.0.0.1:3306)/atone_hands_on?parseTime=true"
)

func main() {
	ctx := context.TODO()

	// Kinesisクライアントの初期化
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

	// UIサーバーのWebSocketに接続
	ws, _, err := websocket.DefaultDialer.Dial("ws://localhost:8080/ws", nil)
	if err != nil {
		log.Fatalf("failed to connect to WebSocket server: %v", err)
	}
	defer ws.Close()

	// ストリームのシャードIDを取得
	shards, err := getShards(ctx, kinesisClient)
	if err != nil {
		log.Fatalf("failed to get shards: %v", err)
	}
	if len(shards) == 0 {
		log.Fatal("no shards found")
	}
	shardID := aws.ToString(shards[0].ShardId)

	// イテレーターを作成
	iteratorOutput, err := kinesisClient.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardID),
		ShardIteratorType: types.ShardIteratorTypeLatest, //ここを TrimHorizon から Latest に変更
		StreamName:        aws.String(streamName),
	})
	if err != nil {
		log.Fatalf("failed to get shard iterator: %v", err)
	}
	shardIterator := iteratorOutput.ShardIterator

	log.Println("Listening for events...")

	// レコードの取得ループ
	for {
		recordsOutput, err := kinesisClient.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})
		if err != nil {
			log.Fatalf("failed to get records: %v", err)
		}

		if len(recordsOutput.Records) > 0 {
			for _, record := range recordsOutput.Records {
				processEvent(ctx, db, kinesisClient, record, ws)
			}
		}

		shardIterator = recordsOutput.NextShardIterator
		if shardIterator == nil {
			log.Println("Shard iterator is nil. Exiting.")
			break
		}

		time.Sleep(1 * time.Second)
	}
}

// getShardsはストリームのシャード情報を取得するヘルパー関数
func getShards(ctx context.Context, client *kinesis.Client) ([]types.Shard, error) {
	output, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String(streamName)})
	if err != nil {
		return nil, err
	}
	return output.StreamDescription.Shards, nil
}

// processEventは受信したイベントを処理する
func processEvent(ctx context.Context, db *sql.DB, kinesisClient *kinesis.Client, record types.Record, ws *websocket.Conn) {
	log.Printf("Received event: %s\n", record.Data)

	var genericEvent map[string]interface{}
	if err := json.Unmarshal(record.Data, &genericEvent); err != nil {
		log.Printf("Failed to unmarshal generic event: %v", err)
		return
	}

	// イベントをeventsテーブルに保存
	saveEventToDB(db, record.Data, genericEvent["event_type"].(string))

	// イベントタイプに応じて処理を分岐
	switch genericEvent["event_type"] {
	case "PurchaseCompletedEvent":
		var purchaseEvent events.PurchaseCompletedEvent
		if err := json.Unmarshal(record.Data, &purchaseEvent); err != nil {
			log.Printf("Failed to unmarshal purchase event: %v", err)
			return
		}

		// 支払約束イベントを生成
		promiseID := uuid.New().String()
		promiseEvent := events.PaymentPromiseCreatedEvent{
			EventType:   "PaymentPromiseCreatedEvent",
			PromiseID:   promiseID,
			OrderID:     purchaseEvent.OrderID,
			UserID:      purchaseEvent.UserID,
			DueDate:     time.Now().Add(30 * 24 * time.Hour).Format("2006-01-02"), // 30日後の期日を設定
			PaymentMode: "月まとめ払い", // ハンズオンでは固定
		}

		// 新しいイベントをKinesisに発行
		if err := producer.EmitEvent(ctx, kinesisClient, promiseEvent); err != nil {
			log.Printf("Failed to emit payment promise event: %v", err)
		}
	case "PaymentPromiseCreatedEvent":
		var promiseEvent events.PaymentPromiseCreatedEvent
		if err := json.Unmarshal(record.Data, &promiseEvent); err != nil {
			log.Printf("Failed to unmarshal promise event: %v", err)
			return
		}

		savePromiseToDB(db, promiseEvent)
	    log.Printf("Payment promise projection created: %+v", promiseEvent)

	case "MemberBillCreatedEvent":
		var billEvent events.MemberBillCreatedEvent
		if err := json.Unmarshal(record.Data, &billEvent); err != nil {
			log.Printf("Failed to unmarshal bill event: %v", err)
			return
		}

		// member_billsテーブルにプロジェクションを保存（状態: unpaid）
		saveMemberBillToDB(db, billEvent)
		log.Printf("Member bill projection created: %+v", billEvent)

	case "PaymentCompletedEvent":
		var paymentEvent events.PaymentCompletedEvent
		if err := json.Unmarshal(record.Data, &paymentEvent); err != nil {
			log.Printf("Failed to unmarshal payment event: %v", err)
			return
		}

		// member_billsテーブルのステータスを更新
		updateMemberBillStatus(db, paymentEvent)
		log.Printf("Member bill status updated to paid: %+v", paymentEvent)

		// UIサーバーに更新通知を送信
		if err := ws.WriteMessage(websocket.TextMessage, []byte("update")); err != nil {
			log.Printf("Failed to write to WebSocket: %v", err)
		}
	}
}

// saveEventToDBはイベントをeventsテーブルに保存する
func saveEventToDB(db *sql.DB, data []byte, eventType string) {
	query := "INSERT INTO events (id, event_type, event_data) VALUES (?, ?, ?)"
	_, err := db.Exec(query, uuid.New().String(), eventType, string(data))
	if err != nil {
		log.Printf("Failed to save event to DB: %v", err)
	}
}

func savePromiseToDB(db *sql.DB, promise events.PaymentPromiseCreatedEvent) {
	query := "INSERT INTO payment_promises (id, order_id, user_id, amount, due_date, payment_mode) VALUES (?, ?, ?, ?, ?, ?)"
	// このamountはPaymentPromiseCreatedEventにはないので、PurchaseCompletedEventから取得するか、仮の値を入れる必要があります。
	// ここではハンズオンのために、PurchaseCompletedEventの金額を直接保存するロジックを組み込みます。
	// 本来はイベントに含めるべき情報です。
	_, err := db.Exec(query, promise.PromiseID, promise.OrderID, promise.UserID, 3500, promise.DueDate, promise.PaymentMode)
	if err != nil {
		log.Printf("Failed to save promise to DB: %v", err)
	}
}

// saveMemberBillToDBは会員請求をmember_billsテーブルに保存する
func saveMemberBillToDB(db *sql.DB, bill events.MemberBillCreatedEvent) {
	query := "INSERT INTO member_bills (id, promise_id, user_id, amount, status, issued_date) VALUES (?, ?, ?, ?, ?, ?)"
	_, err := db.Exec(query, bill.BillID, bill.PromiseID, bill.UserID, bill.Amount, "unpaid", bill.IssuedDate)
	if err != nil {
		log.Printf("Failed to save member bill to DB: %v", err)
	}
}

// updateMemberBillStatusは会員請求のステータスを更新する
func updateMemberBillStatus(db *sql.DB, payment events.PaymentCompletedEvent) {
	query := "UPDATE member_bills SET status = ?, paid_date = ? WHERE id = ?"
	_, err := db.Exec(query, "paid", payment.PaidDate, payment.BillID)
	if err != nil {
		log.Printf("Failed to update member bill status: %v", err)
	}
}