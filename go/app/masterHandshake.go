package main

import (
	"fmt"
	"net"
	"strings"
)

// getting the repl node
// func getRepl() *ReplNode {

// 	for _, r := range replNodes {
// 		if r.Addr == addr {
// 			return &r
// 		}
// 	}
// 	r := ReplNode{Addr: addr}
// 	replNodes = append(replNodes, r)
// 	return &r
// }

func pingRequest(conn net.Conn) error {
	writer := NewWriter(conn)
	writer.Write(Value{typ: "array", array: []Value{{typ: "bulk", bulk: "PING"}}})
	resp := NewResp(conn)
	_, err := resp.Read()
	if err != nil {
		return err
	}
	return nil
}

func replConfPortRequest(conn net.Conn, port int) error {
	pstr := fmt.Sprintf("%d", port)
	writer := NewWriter(conn)
	writer.Write(Value{typ: "array", array: []Value{{typ: "bulk", bulk: "REPLCONF"}, {typ: "bulk", bulk: "listening-port"}, {typ: "bulk", bulk: pstr}}})
	resp := NewResp(conn)
	_, err := resp.Read()
	if err != nil {
		return err
	}
	return nil
}

func replConfCapacityRequest(conn net.Conn, capa string) error {
	writer := NewWriter(conn)
	writer.Write(Value{typ: "array", array: []Value{{typ: "bulk", bulk: "REPLCONF"}, {typ: "bulk", bulk: "capa"}, {typ: "bulk", bulk: capa}}})
	resp := NewResp(conn)
	_, err := resp.Read()
	if err != nil {
		return err
	}
	return nil
}

func psyncRequest(conn net.Conn) error {
	writer := NewWriter(conn)
	writer.Write(Value{typ: "array", array: []Value{{typ: "bulk", bulk: "PSYNC"}, {typ: "bulk", bulk: "?"}, {typ: "bulk", bulk: "-1"}}})
	resp := NewResp(conn)
	val, err := resp.Read()

	if err != nil {
		return err
	}

	if val.typ != "bulk" {
		return fmt.Errorf("expected bulk, got %s", val.typ)
	}

	if !strings.Contains(val.bulk, "+FULLRESYNC") {
		return fmt.Errorf("wrong psync response")
	}

	slice := strings.Split(val.bulk, "\r\n")
	if len(slice) == 0 {
		return fmt.Errorf("wrong psync response lines")
	}
	cs := strings.Split(slice[0], " ")
	if len(cs) != 3 {
		return fmt.Errorf("wrong psync response items")
	}

	return nil

}
