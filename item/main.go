package main

import (
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
	statusTried status = iota
	statusCanceled
	statusConfirmed
)

const (
	addr = "127.0.0.1:9003"
	dsn  = "root:devilsm8875@tcp(127.0.0.1:3306)/item"
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

	mux := mux.NewRouter()
	mux.HandleFunc("/try", s.try)
	mux.HandleFunc("/cancel", s.cancel)
	mux.HandleFunc("/confirm", s.confirm)
	httpSrv := &http.Server{Addr: addr, Handler: mux}
	if err := httpSrv.ListenAndServe(); err != nil {
		panic(err)
	}
}

type bizData struct {
	OrderID int64
	UserID  int64
	ItemID  int64
	Price   int64
}

type event struct {
	ID     int64
	BizID  int64
	Status status
}

func (s *server) try(w http.ResponseWriter, r *http.Request) {
	bizData := &bizData{}
	if err := json.NewDecoder(r.Body).Decode(bizData); err != nil {
		logrus.Error(err)
		return
	}

	tx, err := s.db.Begin()
	if err != nil {
		logrus.Error(err)
		return
	}

	defer tx.Rollback()
	if _, err := tx.Exec("insert into event(biz_id, status) values(?,?)", bizData.OrderID, statusTried); err != nil {
		if isDup(err) {
			return
		}
		logrus.Error(err)
		return
	}

	if _, err := tx.Exec("update item set amount = amount - ? where id = ? and amount >= ?", 1, bizData.ItemID, 1); err != nil {
		logrus.Error(err)
		return
	}

	if _, err := tx.Exec("insert into item_tmp(biz_id, amount) values(?,?)", bizData.OrderID, 1); err != nil {
		logrus.Error(err)
		return
	}
	if err := tx.Commit(); err != nil {
		logrus.Error(err)
		return
	}

}

func (s *server) cancel(w http.ResponseWriter, r *http.Request) {
	bizData := &bizData{}
	if err := json.NewDecoder(r.Body).Decode(bizData); err != nil {
		logrus.Error(err)
		return
	}
	tx, err := s.db.Begin()
	if err != nil {
		logrus.Error(err)
		return
	}
	defer tx.Rollback()
	event := &event{}
	if err := tx.QueryRow("select id,biz_id, status from event where biz_id = ? for update", bizData.OrderID).Scan(&event.ID, &event.BizID, &event.Status); err != nil {
		if err == sql.ErrNoRows {
			if _, err := tx.Exec("insert into event(biz_id, status) values(?,?)", bizData.OrderID, statusCanceled); err != nil {
				return
			}
			return
		}
		logrus.Error(err)
		return
	}
	if event.Status != statusTried {
		return
	}
	if _, err := tx.Exec("delete from item_tmp where biz_id = ?", bizData.OrderID); err != nil {
		logrus.Error(err)
		return
	}
	if _, err := tx.Exec("update item set amount = amount + ? where id = ?", 1, bizData.ItemID); err != nil {
		logrus.Error(err)
		return
	}
	if _, err := tx.Exec("update event set status = ? where id = ?", statusCanceled, event.ID); err != nil {
		logrus.Error(err)
		return
	}

	if err := tx.Commit(); err != nil {
		logrus.Error(err)
		return
	}

}

func (s *server) confirm(w http.ResponseWriter, r *http.Request) {
	bizData := &bizData{}
	if err := json.NewDecoder(r.Body).Decode(bizData); err != nil {
		logrus.Error(err)
		return
	}
	tx, err := s.db.Begin()
	if err != nil {
		logrus.Error(err)
		return
	}
	defer tx.Rollback()
	event := &event{}
	if err := tx.QueryRow("select id,biz_id, status from event where biz_id = ? for update", bizData.OrderID).Scan(&event.ID, &event.BizID, &event.Status); err != nil {
		logrus.Error(err)
		return
	}
	if event.Status != statusTried {
		return
	}
	if _, err := tx.Exec("delete from item_tmp where biz_id = ?", bizData.OrderID); err != nil {
		logrus.Error(err)
		return
	}
	if _, err := tx.Exec("update item set amount = amount + ? where id = ?", 1, bizData.ItemID); err != nil {
		logrus.Error(err)
		return
	}
	if _, err := tx.Exec("update event set status = ? where id = ?", statusConfirmed, event.ID); err != nil {
		logrus.Error(err)
		return
	}

	if err := tx.Commit(); err != nil {
		logrus.Error(err)

		return
	}

}

func isDup(err error) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == duplicateErrorNumber
	}
	return false
}
