package main

import (
	"log"
	"time"
)

type Order struct {
	ItemID   int64     `json:"ItemID"`
	ItemName string    `json:"ItemName"`
	Date     time.Time `json:"Date"`
	Price    float64   `json:"Price"`
	Status   string    `json:"status"`
}

// maybe extract this up?
func ProcessOrderPayment(order Order) error {
	log.Println("### processing order payment ###")
	time.Sleep(time.Second * 2)
	log.Println("### writing order payment to database ###")
	time.Sleep(time.Second * 1)
	return nil
}
