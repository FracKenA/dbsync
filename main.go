package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Create database handle and confirm driver is present.
	user := "user"
	pass := "pass"
	system := "system"
	database := "database"
	// DSN format "<user>:<pass>@tcp(<system>)/<database>"
	DSN := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, pass, system, database)

	db, err := sql.Open("mysql", DSN)
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()

	// Connect and check the server version
	var version string
	db.QueryRow("SELECT VERSION()").Scan(&version)
	fmt.Println("Connected to:", version)
}

// References: https://mariadb.com/resources/blog/using-go-with-mariadb
