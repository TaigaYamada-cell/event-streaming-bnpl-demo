package commands

// PurchaseCommandは、ユーザーの商品購入の意図を表す
type PurchaseCommand struct {
	UserID      string `json:"user_id"`
	ProductID   string `json:"product_id"`
	ProductName string `json:"product_name"`
	Amount      int    `json:"amount"`
}

// StartPaymentCommandは、ユーザーが支払い手続きを開始する意図を表す
type StartPaymentCommand struct {
	UserID    string `json:"user_id"`
	PromiseID string `json:"promise_id"`
}

type CreateMemberBillCommand struct {
	PromiseID string `json:"promise_id"`
	UserID    string `json:"user_id"`
	Amount    int    `json:"amount"`
}