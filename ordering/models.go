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

func ProcessOrder(order Order) error {
	log.Println("### processing order ###")
	// processing is checking if itemID/name is in the store
	// checking if price matches
	// then writing new order to order table
	time.Sleep(time.Second * 1)
	log.Println("### writing order to database ###")
	time.Sleep(time.Second * 1)
	return nil
}
