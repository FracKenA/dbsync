package main

import (
	"database/sql"
	"fmt"
	"flag"
	"log"
	_ "github.com/go-sql-driver/mysql"
)

var user string
var pass string
var server string
var database string
var port int

func init() {
	flag.StringVar(&user, "u", "", "User account for the database.")
	flag.StringVar(&user, "user", "", "User account for the database. (long)")
	flag.StringVar(&pass, "p", "", "Password for the database.")
	flag.StringVar(&pass, "pass", "", "Password for the database. (long)")
	flag.StringVar(&server, "s", "", "Server for the databse.")
	flag.StringVar(&server, "server", "127.0.0.1", "Server for the databse. (long)")
	flag.StringVar(&database, "d", "", "Database on the database server.")
	flag.StringVar(&database, "database", "", "Database on the database server. (long)")

	flag.IntVar(&port, "P", 3306, "Port to connect to.")
	flag.IntVar(&port, "port", 3306, "Port to connect to. (long)")
}

func main() {
	flag.Parse()

	// Create database handle and confirm driver is present.
	// DSN format "<user>:<pass>@tcp(<server>)/<database>"
	dataSource := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, pass, server, port, database)

	db, err := sql.Open("mysql", dataSource)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Connect and check the server version
	var version string
	db.QueryRow("SELECT VERSION()").Scan(&version)
	fmt.Printf("DSN: tcp://%s@%s:%d/%s Version: %s\n", user, server, port, database, version)

	db.Query
}

// References: https://mariadb.com/resources/blog/using-go-with-mariadb
