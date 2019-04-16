package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

func traceCallback(info sqlite3.TraceInfo) int {
	// Not very readable but may be useful; uncomment next line in case of doubt:
	//fmt.Printf("Trace: %#v\n", info)

	var dbErrText string
	if info.DBError.Code != 0 || info.DBError.ExtendedCode != 0 {
		dbErrText = fmt.Sprintf("; DB error: %#v", info.DBError)
	} else {
		dbErrText = "."
	}

	// Show the Statement-or-Trigger text in curly braces ('{', '}')
	// since from the *paired* ASCII characters they are
	// the least used in SQL syntax, therefore better visual delimiters.
	// Maybe show 'ExpandedSQL' the same way as 'StmtOrTrigger'.
	//
	// A known use of curly braces (outside strings) is
	// for ODBC escape sequences. Not likely to appear here.
	//
	// Template languages, etc. don't matter, we should see their *result*
	// at *this* level.
	// Strange curly braces in SQL code that reached the database driver
	// suggest that there is a bug in the application.
	// The braces are likely to be either template syntax or
	// a programming language's string interpolation syntax.

	var expandedText string
	if info.ExpandedSQL != "" {
		if info.ExpandedSQL == info.StmtOrTrigger {
			expandedText = " = exp"
		} else {
			expandedText = fmt.Sprintf(" expanded {%q}", info.ExpandedSQL)
		}
	} else {
		expandedText = ""
	}

	// SQLite docs as of September 6, 2016: Tracing and Profiling Functions
	// https://www.sqlite.org/c3ref/profile.html
	//
	// The profile callback time is in units of nanoseconds, however
	// the current implementation is only capable of millisecond resolution
	// so the six least significant digits in the time are meaningless.
	// Future versions of SQLite might provide greater resolution on the profiler callback.

	var runTimeText string
	if info.RunTimeNanosec == 0 {
		if info.EventCode == sqlite3.TraceProfile {
			//runTimeText = "; no time" // seems confusing
			runTimeText = "; time 0" // no measurement unit
		} else {
			//runTimeText = "; no time" // seems useless and confusing
		}
	} else {
		const nanosPerMillisec = 1000000
		if info.RunTimeNanosec%nanosPerMillisec == 0 {
			runTimeText = fmt.Sprintf("; time %d ms", info.RunTimeNanosec/nanosPerMillisec)
		} else {
			// unexpected: better than millisecond resolution
			runTimeText = fmt.Sprintf("; time %d ns!!!", info.RunTimeNanosec)
		}
	}

	var modeText string
	if info.AutoCommit {
		modeText = "-AC-"
	} else {
		modeText = "+Tx+"
	}

	fmt.Printf("Trace: ev %d %s conn 0x%x, stmt 0x%x {%q}%s%s%s\n",
		info.EventCode, modeText, info.ConnHandle, info.StmtHandle,
		info.StmtOrTrigger, expandedText,
		runTimeText,
		dbErrText)
	return 0
}

func main() {
	eventMask := sqlite3.TraceStmt | sqlite3.TraceProfile | sqlite3.TraceRow | sqlite3.TraceClose

	sql.Register("sqlite3_tracing",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				err := conn.SetTrace(&sqlite3.TraceConfig{
					Callback:        traceCallback,
					EventMask:       eventMask,
					WantExpandedSQL: true,
				})
				return err
			},
		})

	os.Exit(dbMain(os.Args))
}

func dbMain(args []string) int {
	db, err := sql.Open("sqlite3_tracing", "./test.db")
	if err != nil {
		fmt.Printf("Failed to open database: %#+v\n", err)
		return 1
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	tx, err := db.Begin()
	if err != nil {
		log.Panic(err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("select token, user_id, device_id from token as t inner join (select id from user where user_name = ?) as u on u.id = t.user_id")
	if err != nil {
		log.Printf("prepare select token got error: %s\n", err)
		log.Panic(err)
	}
	defer stmt.Close()

	var (
		tokenQuery string
		userid     int
		deviceid   int
	)
	err = stmt.QueryRowContext(ctx, "alice").Scan(&tokenQuery, &userid, &deviceid)
	if err != nil {
		log.Printf("query context got error: %s\n", err)
		log.Panic(err)
	}

	fmt.Println("--------- complete --------")
	if err := tx.Commit(); err != nil {
		log.Panic(err)
	}
	return 1
}
