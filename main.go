package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

var user string
var pass string
var server string
var database string
var port int

type DbRow struct {
	Timestamp          uint
	EventType          uint
	Flags              uint
	Attrib             uint
	Hostname           string
	ServiceDescription string
	State              uint
	Hard               uint
	Retry              uint
	DowntimeDepth      uint
	Output             string
	OutputLong         string
}

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

	//var results DbRow
	var hostnames []string

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

	rows, err := db.Query("select host_name from report_data")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var result string
		err := rows.Scan(&result)
		if err != nil {
			log.Fatal(err)
		}
		hostnames = append(hostnames, result)
	}

	fmt.Println(strings.Join(hostnames, ","))

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	//rows, err := db.Query("delete from report_data where host_name like(?)", strings.Join(hostnames, ","))
}

// References: https://mariadb.com/resources/blog/using-go-with-mariadb
