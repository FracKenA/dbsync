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
var server1 string
var server2 string
var database1 string
var database2 string

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
	flag.StringVar(&server1, "s1", "127.0.0.1", "Source server for the databse. server:port#")
	flag.StringVar(&server1, "server1", "127.0.0.1", "Source server for the databse. server:port# (long)")
	flag.StringVar(&server2, "s2", "127.0.0.1", "Destination Server for the databse. server:port#")
	flag.StringVar(&server2, "server2", "127.0.0.1", "Destination server for the databse. server:port# (long)")
	flag.StringVar(&database1, "d1", "", "Database on the servers.")
	flag.StringVar(&database1, "database1", "", "Database on the servers. (long)")
	flag.StringVar(&database2, "d2", "", "Database on the servers.")
	flag.StringVar(&database2, "database2", "", "Database on the servers. (long)")
}

func main() {
	flag.Parse()

	var hostnames []string

	// Create database handle and confirm driver is present.
	// DSN format "<user>:<pass>@tcp(<server>)/<database>"
	dataSource := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, pass, server1, database1)
	dataDestination := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, pass, server2, database2)

	db1, err := sql.Open("mysql", dataSource)
	if err != nil {
		log.Fatal(err)
	}
	defer db1.Close()

	err = db1.Ping()
	if err != nil {
		log.Fatal(err)
	}

	db2, err := sql.Open("mysql", dataDestination)
	if err != nil {
		log.Fatal(err)
	}
	defer db2.Close()

	err = db2.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// Query server1 to get the rows.
	rows, err := db1.Query("select distinct host_name from report_data where host_name != '' order by host_name")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var host string
		err := rows.Scan(&host)
		if err != nil {
			log.Fatal(err)
		}
		hostnames = append(hostnames, host)
	}

	log.Printf("Hosts in poller: %s\n", strings.Join(hostnames, ", "))

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	tx, err := db2.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("delete from report_data where host_name = ?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	for _, host := range hostnames {
		fmt.Printf("Deleting host %s...\t", host)
		results, err := stmt.Exec(host)
		if err != nil {
			log.Fatal(err)
		}

		rowsAffected, err := results.RowsAffected()
		if err != nil {
			log.Printf("Error with rows affecteed, %s\n", err)
		}
		log.Printf("%d rows affected.\n", rowsAffected)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	for _, host := range hostnames {
		fmt.Println(host)
		rows, err := db1.Query("select timestamp, event_type, flags, attrib, host_name, service_description, state, hard, retry, downtime_depth, output, long_output from report_data where host_name = ?", host)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
			var timestamp, event_type, attrib, state, hard, retry, downtime_depth uint64
			var flags, host_name, service_description, output, long_output string
			err = rows.Scan(&timestamp, &event_type, &flags, &attrib, &host_name, &service_description, &state, &hard, &retry, &downtime_depth, &output, &long_output)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("%d %d %d %s %s %d %d %d %d %s %s", timestamp, event_type, flags, attrib, host_name, service_description, state, hard, retry, downtime_depth, output, long_output)
		}
	}
}

// References:
//   https://mariadb.com/resources/blog/using-go-with-mariadb
//   http://go-database-sql.org
