package main

import (
	"fmt"
)
type User struct {
	Id string `json:"id"`
	UserName string `json:"name"`
	FirstName string `json:"firstName"`
	LastName string `json:"lastName"`
}

func main() {
	fmt.Println("Welcome to google golang.org")


}
