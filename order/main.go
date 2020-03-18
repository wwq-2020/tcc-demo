package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/sirupsen/logrus"

	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

const duplicateErrorNumber = 1062

type status int

const (
	statusInit status = iota
	statusTried
	statusCanceled
	statusConfirmed
)

const (
	addr = "127.0.0.1:9001"
	dsn  = "root:devilsm8875@tcp(127.0.0.1:3306)/order"
)

type server struct {
	db *sql.DB
}

func main() {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	s := &server{db: db}

	// 实际场景可以考虑分布式锁,减少不必要的多实例恢复
	go s.recover()

	mux := mux.NewRouter()
	mux.HandleFunc("/purchase", s.purchase)
	httpSrv := &http.Server{Addr: addr, Handler: mux}
	if err := httpSrv.ListenAndServe(); err != nil {
		panic(err)
	}
}

type bizData struct {
	OrderID int64
	ItemID  int64
	Price   int64
	UserID  int64
}

type event struct {
	ID      int64
	Status  status
	BizData string
}

func (s *server) purchase(w http.ResponseWriter, r *http.Request) {
	// 这边省去一些流程,因为只是demo
	// 我们假定订单id是1,价格是10,商品id是1,用户ID是1
	bizData := &bizData{OrderID: 1, ItemID: 1, Price: 10, UserID: 1}
	data, err := json.Marshal(bizData)
	if err != nil {
		logrus.Error(err)
		return
	}

	result, err := s.db.Exec("insert into event(biz_id, biz_data, status) values(?,?,?)", bizData.OrderID, string(data), statusInit)
	if err != nil {
		if !isDup(err) {
			logrus.Error(err)
			return
		}
	}

	id, err := result.LastInsertId()
	if err != nil {
		logrus.Error(err)
		return
	}

	if err := s.tryAccount(bizData); err != nil {
		go s.cancelAccount2Success(bizData)
		logrus.Error(err)
		return
	}

	if err := s.tryItem(bizData); err != nil {
		go func() {
			s.cancel2Success(id, bizData)
			s.setEventStatus2Success(id, statusCanceled)
		}()
		logrus.Error(err)
		return
	}

	if _, err := s.db.Exec("update event set status = ? where id = ?", statusTried, id); err != nil {
		go func() {
			s.setEventStatus2Success(id, statusTried)
			s.confirm2Success(id, bizData)
			s.setEventStatus2Success(id, statusConfirmed)
		}()
		logrus.Error(err)
		return
	}

	if err := s.confirmAccount(bizData); err != nil {
		go func() {
			s.confirmAccount2Success(bizData)
			s.setEventStatus2Success(id, statusConfirmed)
		}()
		logrus.Error(err)
		return
	}

	if err := s.confirmItem(bizData); err != nil {
		go func() {
			s.confirmItem2Success(bizData)
			s.setEventStatus2Success(id, statusConfirmed)
		}()
		logrus.Error(err)
		return
	}

	if _, err := s.db.Exec("update event set status = ? where id = ?", statusConfirmed, id); err != nil {
		logrus.Error(err)
		go s.setEventStatus2Success(id, statusConfirmed)
		return
	}

}

func (s *server) recover() {
	for {
		offset := s.findOffset()
		events := s.findEvents(offset)
		if len(events) == 0 {
			return
		}
		s.handleEvents(events)
		s.updateOffset(offset + int64(len(events)))
	}
}

func (s *server) findOffset() int64 {
	var offset int64
	for {
		if err := s.db.QueryRow("select offset from event_scan_offset").Scan(&offset); err == nil || err == sql.ErrNoRows {
			return offset
		}
	}
}

func (s *server) findEvents(offset int64) []*event {
	for {
		events, err := s.doFindEvents(offset)
		if err != nil {
			continue
		}
		return events
	}
}

func (s *server) doFindEvents(offset int64) ([]*event, error) {
	rows, err := s.db.Query("select biz_data,status from event where id > ? limit 1000", offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []*event
	for rows.Next() {
		result := &event{}
		if err := rows.Scan(&result.BizData, &result.Status); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *server) handleEvents(events []*event) {
	for _, event := range events {
		s.handleEvent(event)
	}
}

func (s *server) handleEvent(event *event) {
	for {
		if err := s.doHandleEvent(event); err == nil {
			return
		}
	}
}

func (s *server) doHandleEvent(event *event) error {
	bizData := &bizData{}
	if err := json.Unmarshal([]byte(event.BizData), event); err != nil {
		logrus.Error(err)
		return nil
	}
	switch event.Status {
	case statusInit:
		s.cancel2Success(event.ID, bizData)
	case statusTried:
		s.confirm2Success(event.ID, bizData)
	case statusConfirmed:
	case statusCanceled:
	default:
		panic("unknown status")
	}
	return nil
}

func (s *server) cancel2Success(id int64, bizData *bizData) {
	s.cancelAccount2Success(bizData)
	s.cancelItem2Success(bizData)
	s.setEventStatus2Success(id, statusCanceled)

}

func (s *server) cancelAccount2Success(bizData *bizData) {
	for {
		if err := s.cancelAccount(bizData); err == nil {
			break
		}
	}
}

func (s *server) cancelItem2Success(bizData *bizData) {
	for {

		if err := s.cancelItem(bizData); err == nil {
			break
		}
	}
}

func (s *server) setEventStatus2Success(id int64, status status) {
	for {
		if _, err := s.db.Exec("update event set status = ? where id = ?", status, id); err == nil {
			break
		}
	}
}

func (s *server) confirm2Success(id int64, bizData *bizData) {
	s.confirmAccount2Success(bizData)
	s.confirmItem2Success(bizData)
	s.setEventStatus2Success(id, statusConfirmed)
}

func (s *server) confirmAccount2Success(bizData *bizData) {
	for {
		if err := s.confirmAccount(bizData); err == nil {
			break
		}
	}
}

func (s *server) confirmItem2Success(bizData *bizData) {
	for {
		if err := s.confirmAccount(bizData); err == nil {
			break
		}
	}
}

func (s *server) tryAccount(bizData *bizData) error {
	data, _ := json.Marshal(bizData)
	resp, err := http.Post("http://127.0.0.1:9002/try", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (s *server) tryItem(bizData *bizData) error {
	data, _ := json.Marshal(bizData)
	resp, err := http.Post("http://127.0.0.1:9003/try", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (s *server) cancelAccount(bizData *bizData) error {
	data, _ := json.Marshal(bizData)
	resp, err := http.Post("http://127.0.0.1:9002/cancel", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (s *server) cancelItem(bizData *bizData) error {
	data, _ := json.Marshal(bizData)
	resp, err := http.Post("http://127.0.0.1:9003/cancel", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (s *server) confirmAccount(bizData *bizData) error {
	data, _ := json.Marshal(bizData)
	resp, err := http.Post("http://127.0.0.1:9002/confirm", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (s *server) confirmItem(bizData *bizData) error {
	data, _ := json.Marshal(bizData)
	resp, err := http.Post("http://127.0.0.1:9003/confirm", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (s *server) updateOffset(offset int64) {
	for {
		if _, err := s.db.Exec("insert into event_scan_offset(id,offset) values(1,?) on duplicate key update offset = ?", offset, offset); err == nil {
			return
		}
	}
}

func isDup(err error) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == duplicateErrorNumber
	}
	return false
}
