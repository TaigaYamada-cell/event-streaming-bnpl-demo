package events

// PurchaseCompletedEventは、商品購入が完了した事実を表す
type PurchaseCompletedEvent struct {
	EventType string `json:"event_type"`
	OrderID   string `json:"order_id"`
	UserID    string `json:"user_id"`
	Amount    int    `json:"amount"`
}

// PaymentPromiseCreatedEventは、支払約束が作成された事実を表す
type PaymentPromiseCreatedEvent struct {
	EventType    string `json:"event_type"`
	PromiseID    string `json:"promise_id"`
	OrderID      string `json:"order_id"`
	UserID       string `json:"user_id"`
	DueDate      string `json:"due_date"`
	PaymentMode  string `json:"payment_mode"` // "すぐ払い" or "月まとめ払い"
}

// MemberBillCreatedEventは、会員への請求が行われた事実を表す
type MemberBillCreatedEvent struct {
	EventType  string `json:"event_type"`
	BillID     string `json:"bill_id"`
	PromiseID  string `json:"promise_id"`
	UserID     string `json:"user_id"`
	Amount     int    `json:"amount"`
	IssuedDate string `json:"issued_date"`
}

// PaymentCompletedEventは、会員による支払いが完了した事実を表す
type PaymentCompletedEvent struct {
	EventType string `json:"event_type"`
	BillID    string `json:"bill_id"`
	UserID    string `json:"user_id"`
	Amount    int    `json:"amount"`
	PaidDate  string `json:"paid_date"`
}
