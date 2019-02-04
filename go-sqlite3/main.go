package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"

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

// Harder to do DB work in main().
// It's better with a separate function because
// 'defer' and 'os.Exit' don't go well together.
//
// DO NOT use 'log.Fatal...' below: remember that it's equivalent to
// Print() followed by a call to os.Exit(1) --- and
// we want to avoid Exit() so 'defer' can do cleanup.
// Use 'log.Panic...' instead.

func daysInMonth(year int, m time.Month) int {
	return time.Date(year, m+1, 0, 0, 0, 0, 0, time.UTC).Day()
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
	for _, username := range []string{"alice", "bob"} {
		listAssetsYearMonth(db, username, "1234567")
		for y := 2007; y < 2020; y++ {
			listAssetsYear(db, username, "1234567", y)
			for m := 1; m <= 12; m++ {
				listAssetsByMonth(db, username, "1234567", y, m)
				for d := 1; d <= 31; d++ {
					days := daysInMonth(y, time.Month(m))
					if d > days {
						break
					}
					listAssetsByDay(db, username, "1234567", y, m, d)
				}
			}
		}
	}

	fmt.Println("--------- complete --------")
	return 0
}

func inQuery(db *sql.DB, fun func(ctx context.Context, tx *sql.Tx) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := fun(ctx, tx); err != nil {
		log.Printf("query got error: %v", err)
		if err := tx.Rollback(); err != nil {
			log.Printf("During rollback: %v", err)
		}

		return err
	}

	return tx.Commit()
}

func validateToken(ctx context.Context, tx *sql.Tx, username, token string) (int, int, error) {
	stmt, err := tx.Prepare("select token, user_id, device_id from token as t inner join (select id from user where user_name = ?) as u on u.id = t.user_id")
	if err != nil {
		log.Printf("prepare select token got error: %s\n", err)
		return -1, -1, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, username)
	if err != nil {
		log.Printf("query context got error: %s\n", err)
		return -1, -1, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			tokenQuery string
			userid     int
			deviceid   int
		)
		if err := rows.Scan(&tokenQuery, &userid, &deviceid); err == nil {
			// TODO: check expire time
			//logrus.Println("TODO: not check token expire date yet")

			if token == tokenQuery {
				return userid, deviceid, nil
			}
		} else {
			log.Printf("scan got error: %v", err)
			return -1, -1, err
		}
	}

	return -1, -1, errors.New("token not found")
}

func listAssetsYearMonth(db *sql.DB, username, token string) {
	if err := inQuery(db, func(ctx context.Context, tx *sql.Tx) error {
		userid, _, err := validateToken(ctx, tx, username, token)
		if err != nil {
			return err
		}
		err = getAssetsByYears(ctx, tx, userid)
		return err
	}); err != nil {
		log.Panic(err)
	}
}

func listAssetsYear(db *sql.DB, username, token string, year int) {
	if err := inQuery(db, func(ctx context.Context, tx *sql.Tx) error {
		userid, _, err := validateToken(ctx, tx, username, token)
		if err != nil {
			return err
		}
		err = getAssetsByYear(ctx, tx, userid, year, true)
		return err
	}); err != nil {
		log.Panic(err)
	}
}

func listAssetsByMonth(db *sql.DB, username, token string, year, month int) {
	if err := inQuery(db, func(ctx context.Context, tx *sql.Tx) error {
		userid, _, err := validateToken(ctx, tx, username, token)
		if err != nil {
			return err
		}
		err = getAssetsByMonth(ctx, tx, userid, year, month)
		return err
	}); err != nil {
		log.Panic(err)
	}
}

func listAssetsByDay(db *sql.DB, username, token string, year, month, day int) {
	if err := inQuery(db, func(ctx context.Context, tx *sql.Tx) error {
		userid, _, err := validateToken(ctx, tx, username, token)
		if err != nil {
			return err
		}
		err = getAssetsByDay(ctx, tx, userid, year, month, day)
		return err
	}); err != nil {
		log.Panic(err)
	}
}

func getAssetsByDay(ctx context.Context, tx *sql.Tx, userid, y, m, d int) error {
	stmt, err := tx.Prepare("select id, hash, ext_id from (select * from asset as a inner join (select * from device) as d on a.device_id = d.id where a.user_id = ? and year = ? and month = ? and day = ?) order by hash")
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, userid, y, m, d)
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var (
			assetid int
			extID   int
			hash    string
		)
		err = rows.Scan(&assetid, &hash, &extID)
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func getAssetsDayHash(ctx context.Context, tx *sql.Tx, userid, y, m, d int) error {
	stmt, err := tx.Prepare("select group_concat(hash, '') from (select hash from asset where user_id = ? and year = ? and month = ? and day = ? order by hash)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	hash := ""
	err = stmt.QueryRowContext(ctx, userid, y, m, d).Scan(&hash)
	if err != nil {
		return err
	}

	return nil
}

func getDistinctDays(ctx context.Context, tx *sql.Tx, userid, y, m int) ([]int, error) {
	ds := []int{}
	stmt, err := tx.Prepare("select distinct day as d from asset where user_id = ? and year = ? and month = ? order by d")
	if err != nil {
		return ds, err
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, userid, y, m)
	if err != nil {
		return ds, err
	}

	defer rows.Close()

	for rows.Next() {
		var d int
		err = rows.Scan(&d)
		if err != nil {
			return ds, err
		}
		ds = append(ds, d)
	}
	return ds, rows.Err()
}

func getAssetsByMonth(ctx context.Context, tx *sql.Tx, userid, y, m int) error {
	ds, err := getDistinctDays(ctx, tx, userid, y, m)
	if err != nil {
		return err
	}
	for _, d := range ds {
		if err := getAssetsByDay(ctx, tx, userid, y, m, d); err != nil {
			return err
		}
	}
	return nil
}

func getDaysMap(ctx context.Context, tx *sql.Tx, userid, y int) ([12][31]bool, error) {
	var daysMap [12][31]bool
	stmt, err := tx.Prepare("select distinct month as m, day as d from asset where user_id = ? and year= ? order by m, d")
	if err != nil {
		return daysMap, err
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, userid, strconv.Itoa(y))
	if err != nil {
		return daysMap, err
	}

	defer rows.Close()

	for rows.Next() {
		var m, d int
		err = rows.Scan(&m, &d)
		if err != nil {
			return daysMap, err
		}
		daysMap[m-1][d-1] = true
	}
	return daysMap, rows.Err()
}

func getAssetsByYear(ctx context.Context, tx *sql.Tx, userid, y int, dayDetail bool) error {
	daysMap, err := getDaysMap(ctx, tx, userid, y)
	if err != nil {
		return err
	}

	for m, mm := range daysMap {
		for d, value := range mm {
			if !value {
				continue
			}
			if dayDetail {
				if err := getAssetsByDay(ctx, tx, userid, y, m+1, d+1); err != nil {
					return err
				}
			} else {
				if err := getAssetsDayHash(ctx, tx, userid, y, m+1, d+1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func getDistinctYears(ctx context.Context, tx *sql.Tx, userid int) ([]int, error) {
	ys := []int{}
	stmt, err := tx.Prepare("select distinct year as y from asset where user_id = ? order by y")
	if err != nil {
		return ys, err
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, userid)
	if err != nil {
		return ys, err
	}

	defer rows.Close()

	for rows.Next() {
		var y int
		err = rows.Scan(&y)
		if err != nil {
			return ys, err
		}
		ys = append(ys, y)
	}
	return ys, rows.Err()
}

func getAssetsByYears(ctx context.Context, tx *sql.Tx, userid int) error {
	ys, err := getDistinctYears(ctx, tx, userid)
	if err != nil {
		return err
	}

	for _, y := range ys {
		if err := getAssetsByYear(ctx, tx, userid, y, false); err != nil {
			return err
		}
	}
	return nil
}
