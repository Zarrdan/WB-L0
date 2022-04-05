package main

import (
	"awesomeProject1/Model"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nats-io/stan.go"
	"html/template"
	"log"
	"net/http"
)

var Cache = make(map[string]Model.Orders)

func WriteCachebd() {
	db, err := sqlx.Open("postgres", "user=test password=test dbname=postgres sslmode=disable")
	defer db.Close()
	if err != nil {
		log.Println(err.Error())
		return
	}

	var order []Model.Orders
	var delivery []Model.Delivery
	var payment []Model.Payment
	var item []Model.Items
	err = db.Select(&order, `select order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, 
       shardkey, sm_id, date_created, oof_shard from orders`)
	if err != nil {
		log.Println(err.Error())
	}
	err = db.Select(&delivery, `select name, phone, zip, city, address, region, email from deliveries;`)
	if err != nil {
		log.Println(err.Error())
	}
	err = db.Select(&payment, `select transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, 
       goods_total, custom_fee from payments;`)
	if err != nil {
		log.Println(err.Error())
	}
	err = db.Select(&item, `select chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status from items;`)
	if err != nil {
		log.Println(err.Error())
	}

	var itemIds []string
	rows, err := db.Query(`select order_uid from items`)
	if err != nil {
		log.Println(err.Error())
	}
	for rows.Next() {
		var itemId string
		err = rows.Scan(&itemId)
		if err != nil {
			log.Println(err.Error())
		}
		itemIds = append(itemIds, itemId)
	}

	j := 0
	for i := 0; i < len(order); i++ {
		order[i].Delivery = delivery[i]
		order[i].Payment = payment[i]
		for j < len(itemIds) {
			var itemStruct []Model.Items
			if itemIds[j] == order[i].OrderUid {
				itemStruct = append(itemStruct, item[j])
				j++
			} else {
				order[i].Items = itemStruct
				break
			}
		}
		Cache[order[i].OrderUid] = order[i]
	}
}

func ReadFromChannel() {
	sc, _ := stan.Connect("test-cluster", "ClientID")
	_, err := sc.Subscribe("foo", func(msg *stan.Msg) { // подписываемся на канал и при получении сообщения вызываем WriteData
		err := WriteData(msg)
		if err == nil {
			log.Println("Received a message")
		} else {
			log.Println(err.Error())
		}
	})
	if err != nil {
		log.Println(err.Error())
	}
}

func WriteData(m *stan.Msg) error {
	var order Model.Orders
	err := json.Unmarshal(m.Data, &order)
	if err != nil {
		return err
	}
	if order.OrderUid == "" {
		return fmt.Errorf("Null OrderUid")
	}
	Cache[order.OrderUid] = order

	db, err := sql.Open("postgres", "user=test password=test dbname=postgres sslmode=disable")
	defer db.Close()
	if err != nil {
		return err
	}
	_, err = db.Exec(`insert into orders (order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, 
        sm_id, date_created, oof_shard) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);`,
		order.OrderUid, order.TrackNumber, order.Entry, order.Locale, order.InternalSignature, order.CustomerId, order.DeliveryService,
		order.Shardkey, order.SmId, order.DateCreated, order.OofShard)
	if err != nil {
		delete(Cache, order.OrderUid)
		db.Exec(`delete from orders where order_uid = $1`, order.OrderUid)
		return fmt.Errorf("Insert into orders", err)
	}
	_, err = db.Exec(`insert into deliveries (customer_id, name, phone, zip, city, address, region, email) values ($1, $2, $3, $4, $5, $6, $7, $8);`,
		order.CustomerId, order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip,
		order.Delivery.City, order.Delivery.Address, order.Delivery.Region, order.Delivery.Email)
	if err != nil {
		delete(Cache, order.OrderUid)
		db.Exec(`delete from orders where order_uid = $1`, order.OrderUid)
		return fmt.Errorf("Insert into deliveries", err)
	}
	_, err = db.Exec(`insert into payments (order_uid, transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, 
        goods_total, custom_fee) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);`,
		order.OrderUid, order.Payment.Transaction, order.Payment.RequestId, order.Payment.Currency, order.Payment.Provider, order.Payment.Amount,
		order.Payment.PaymentDt, order.Payment.Bank, order.Payment.DeliveryCost, order.Payment.GoodsTotal, order.Payment.CustomFee)
	if err != nil {
		delete(Cache, order.OrderUid)
		db.Exec(`delete from orders where order_uid = $1`, order.OrderUid)
		return fmt.Errorf("Insert into payments", err)
	}
	for i := 0; i < len(order.Items); i++ {
		_, err = db.Exec(`insert into items (order_uid, chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status) 
			values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			order.OrderUid, order.Items[i].ChrtId, order.Items[i].TrackNumber, order.Items[i].Price, order.Items[i].Rid,
			order.Items[i].Name, order.Items[i].Sale, order.Items[i].Size, order.Items[i].TotalPrice, order.Items[i].NmId, order.Items[i].Brand, order.Items[i].Status)
		if err != nil {
			delete(Cache, order.OrderUid)
			db.Exec(`delete from orders where order_uid = $1`, order.OrderUid)
			return fmt.Errorf("Insert into items", err)
		}
	}
	return err
}

func HomePage(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFiles("Template/fronted.html")
	if err != nil {
		log.Println(err.Error())
		http.Error(w, "Internal server error", 500)
		return
	}
	err = tmpl.Execute(w, nil)
	if err != nil {
		log.Println(err.Error())
		http.Error(w, "Internal server error", 500)
		return
	}
}

func IdPage(w http.ResponseWriter, r *http.Request) {
	needId := r.URL.Query().Get("id")
	if _, ok := Cache[needId]; ok {
		b, _ := json.Marshal(Cache[needId])
		_, err := w.Write(b)
		if err != nil {
			log.Println(err.Error())
		}
	} else {
		_, err := w.Write([]byte("Запись не найдена"))
		if err != nil {
			log.Println(err.Error())
		}
	}
}

func DataListPage(w http.ResponseWriter, r *http.Request) {
	outputArray := make([]Model.Orders, 0)
	for _, elem := range Cache {
		outputArray = append(outputArray, elem)
	}

	b, _ := json.Marshal(outputArray)
	_, err := w.Write(b)
	if err != nil {
		log.Println(err.Error())
	}
}

func CreateStruct() Model.Orders {
	var (
		order = Model.Orders{
			OrderUid:    "b563feb7b2b84b6test",
			TrackNumber: "WBILMTESTTRACK",
			Entry:       "WBIL",
			Delivery: Model.Delivery{
				Name:    "Test Testov",
				Phone:   "+9720000000",
				Zip:     "2639809",
				City:    "Kiryat Mozkin",
				Address: "Ploshad Mira 15",
				Region:  "Kraiot",
				Email:   "test@gmail.com",
			},
			Payment: Model.Payment{
				Transaction:  "b563feb7b2b84b6test",
				RequestId:    "",
				Currency:     "USD",
				Provider:     "wbpay",
				Amount:       1817,
				PaymentDt:    1637907727,
				Bank:         "alpha",
				DeliveryCost: 1500,
				GoodsTotal:   317,
				CustomFee:    0,
			},
			Locale:            "en",
			InternalSignature: "",
			CustomerId:        "test",
			DeliveryService:   "meest",
			Shardkey:          "9",
			SmId:              99,
			OofShard:          "1",
		}
	)
	item := Model.Items{
		ChrtId:      9934930,
		TrackNumber: "WBILMTESTTRACK",
		Price:       453,
		Rid:         "ab4219087a764ae0btest",
		Name:        "Mascaras",
		Sale:        30,
		Size:        "0",
		TotalPrice:  317,
		NmId:        2389212,
		Brand:       "Vivienne Sabo",
		Status:      202,
	}
	order.Items = append(order.Items, item)
	return order
}

func run() {

	Cache["1"] = CreateStruct()
	Cache["2"] = CreateStruct()
	WriteCachebd()
	go ReadFromChannel()

}

func main() {

	mux := http.NewServeMux()
	mux.HandleFunc("/", HomePage)
	mux.HandleFunc("/record", IdPage)
	mux.HandleFunc("/list/", DataListPage)

	run()

	log.Println("Запуск сервера...")
	log.Fatal(http.ListenAndServe(":8000", mux))
}
