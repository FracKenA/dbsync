package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var user string
var pass string
var server1 string
var server2 string
var database1 string
var database2 string
var skipdelete bool
var skipinsert bool
var noop bool

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

	flag.BoolVar(&skipdelete, "skip-delete", false, "Skip deleting rows. (Not implemented)")
	flag.BoolVar(&skipinsert, "skip-insert", false, "Skip inserting rows. (Not implemeted)")
	flag.BoolVar(&noop, "nop", false, "No operations. (Not implemented)")
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
		log.Panic(err)
	}
	defer db1.Close()

	if err = db1.Ping(); err != nil {
		log.Panic(err)
	}

	db2, err := sql.Open("mysql", dataDestination)
	if err != nil {
		log.Panic(err)
	}
	defer db2.Close()

	if err = db2.Ping(); err != nil {
		log.Panic(err)
	}

	// Query server1 to get the rows.
	rows, err := db1.Query("select distinct host_name from report_data where host_name != '' order by host_name")
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var host string
		err := rows.Scan(&host)
		if err != nil {
			log.Panic(err)
		}
		hostnames = append(hostnames, host)
	}

	if err = rows.Err(); err != nil {
		log.Panic(err)
	}

	tx, err := db2.Begin()
	if err != nil {
		log.Panic(err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("delete from report_data where host_name = ?")
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()

	for _, host := range hostnames {
		fmt.Printf("Deleting %s...\t", host)

		results, err := stmt.Exec(host)
		if err != nil {
			log.Panic(err)
		}

		rowsAffected, err := results.RowsAffected()
		if err != nil {
			log.Panic(err)
		}
		fmt.Printf("%d rows deleted.\n", rowsAffected)
	}

	if err = stmt.Close(); err != nil {
		log.Panic(err)
	}

	if err = tx.Commit(); err != nil {
		log.Panic(err)
	}

	for _, host := range hostnames {
		var rowsInserted int64

		fmt.Printf("Adding %s...\t", host)

		tx, err = db2.Begin()
		if err != nil {
			log.Panic(err)
		}
		defer tx.Rollback()

		stmt, err := tx.Prepare("insert into report_data(timestamp, event_type, flags, attrib, host_name, service_description, state, hard, retry, downtime_depth, output, long_output) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
			log.Panic(err)
		}
		defer stmt.Close()

		// Move to a function and use a goroutine to increate consurrency.
		rows, err := db1.Query("select timestamp, event_type, flags, attrib, host_name, service_description, state, hard, retry, downtime_depth, output, long_output from report_data where host_name = ?", host)
		if err != nil {
			log.Panic(err)
		}

		defer rows.Close()

		for rows.Next() {
			var flags, attrib, downtime_depth sql.NullInt64
			var output, long_output sql.NullString
			var timestamp, event_type, state, hard, retry int64
			var host_name, service_description string

			err = rows.Scan(&timestamp, &event_type, &flags, &attrib, &host_name, &service_description, &state, &hard, &retry, &downtime_depth, &output, &long_output)
			if err != nil {
				log.Panic(err)
			}

			//fmt.Printf("%d %d %d %s %s %d %d %d %d %s %s\n", timestamp, event_type, flags, attrib, host_name, service_description, state, hard, retry, downtime_depth, output, long_output)
			results, err := stmt.Exec(timestamp, event_type, flags, attrib, host_name, service_description, state, hard, retry, downtime_depth, output, long_output)
			rowsAffected, err := results.RowsAffected()
			if err != nil {
				log.Panic(err)
			}

			rowsInserted += rowsAffected
		}

		fmt.Printf("%d rows inserted\n", rowsInserted)
		rowsInserted = 0

		if err = rows.Err(); err != nil {
			log.Panic(err)
		}

		if err = stmt.Close(); err != nil {
			log.Panic(err)
		}

		if err = tx.Commit(); err != nil {
			log.Panic(err)
		}
	}

	fmt.Println("Work finished", time.Now())
}

// References:
//   https://mariadb.com/resources/blog/using-go-with-mariadb
//   http://go-database-sql.org
