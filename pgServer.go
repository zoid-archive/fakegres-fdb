package main

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/jackc/pgproto3/v2"
	pgquery "github.com/pganalyze/pg_query_go/v5"
)

var dataTypeOIDMap = map[string]uint32{
	"text":            25,
	"pg_catalog.int4": 23,
}

type pgServer struct {
	conn net.Conn
	db   fdb.Transactor
	cfg  config
}

func (pgs pgServer) done(buf []byte, msg string) {
	buf = (&pgproto3.CommandComplete{CommandTag: []byte(msg)}).Encode(buf)
	buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	_, err := pgs.conn.Write(buf)
	if err != nil {
		log.Printf("failed to write query response: %s", err)
	}
}

func (pgs pgServer) writePgResult(res *pgResult) {
	rd := &pgproto3.RowDescription{}
	for i, field := range res.fieldNames {
		rd.Fields = append(rd.Fields, pgproto3.FieldDescription{
			Name:        []byte(field),
			DataTypeOID: dataTypeOIDMap[res.fieldTypes[i]],
		})
	}
	buf := rd.Encode(nil)
	for _, row := range res.rows {
		dr := &pgproto3.DataRow{}
		for _, value := range row {
			// Convert value to string representation for text format
			var bs []byte
			switch v := value.(type) {
			case string:
				bs = []byte(v)
			case nil:
				bs = nil
			default:
				bs = []byte(fmt.Sprintf("%v", v))
			}
			dr.Values = append(dr.Values, bs)
		}

		buf = dr.Encode(buf)
	}

	pgs.done(buf, fmt.Sprintf("SELECT %d", len(res.rows)))
}

func (pgs pgServer) handleStartupMessage(pgconn *pgproto3.Backend) error {
	startupMessage, err := pgconn.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("error receiving startup message: %s", err)
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		// Send required parameter statuses
		buf = (&pgproto3.ParameterStatus{Name: "server_version", Value: "15.0"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "server_encoding", Value: "UTF8"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "TimeZone", Value: "UTC"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"}).Encode(buf)
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = pgs.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %s", err)
		}

		return nil
	case *pgproto3.SSLRequest:
		_, err = pgs.conn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("error sending deny SSL request: %s", err)
		}

		return pgs.handleStartupMessage(pgconn)
	default:
		return fmt.Errorf("unknown startup message: %#v", startupMessage)
	}
}

func (pgs pgServer) handleMessage(pgc *pgproto3.Backend) error {
	msg, receive_err := pgc.Receive()
	if receive_err != nil {
		return fmt.Errorf("error receiving message: %s", receive_err)
	}

	switch t := msg.(type) {
	case *pgproto3.Query:
		stmts, parse_err := pgquery.Parse(t.String)
		if parse_err != nil {
			return fmt.Errorf("error parsing query: %s", receive_err)
		}

		if len(stmts.GetStmts()) > 1 {
			return fmt.Errorf("only make one request at a time")
		}

		stmt := stmts.GetStmts()[0]

		// Handle SELECTs here
		s := stmt.GetStmt().GetSelectStmt()
		var res *pgResult
		var err error
		if s != nil {
			pe := newPgEngine(pgs.db)
			if pgs.cfg.columnar {
				res, err = pe.executeSelectColumnar(s)

			} else {
				res, err = pe.executeSelect(s)
			}

			if err != nil {
				return err
			}

			pgs.writePgResult(res)
			return nil
		} else {
			pe := newPgEngine(pgs.db)
			pe.execute(*stmts)
		}

		pgs.done(nil, strings.ToUpper(strings.Split(t.String, " ")[0])+" ok")
	case *pgproto3.Terminate:
		return nil
	default:
		return fmt.Errorf("received message other than Query from client: %s", msg)
	}

	return nil
}

func (pgs pgServer) handle() {
	pgc := pgproto3.NewBackend(pgproto3.NewChunkReader(pgs.conn), pgs.conn)
	defer pgs.conn.Close()

	err := pgs.handleStartupMessage(pgc)
	if err != nil {
		log.Println(err)
		return
	}

	for {
		err := pgs.handleMessage(pgc)
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func runPgServer(port string, db fdb.Transactor, cfg config) {
	ln, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}

		pc := pgServer{conn, db, cfg}
		go pc.handle()
	}
}
