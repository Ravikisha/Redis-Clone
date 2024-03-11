package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Constants

const (
	OK   = "+OK\r\n"
	NULL = "$-1\r\n"
)

var (
	ErrMissingCommand     = errors.New("command error: missing command")
	ErrUnsupportedCommand = errors.New("command error: unsupported command")
	ErrMissingArguments   = errors.New("command error: not enough arguments")
	ErrInvalidArguments   = errors.New("command error: invalid arguments")
)

// handle the connection
var Handlers = map[string]func([]Value, net.Conn) Value{
	"PING":     ping,
	"ECHO":     echo,
	"SET":      set,
	"GET":      get,
	"HSET":     hset,
	"HGET":     hget,
	"HGETALL":  hgetall,
	"INFO":     info,
	"REPLCONF": replconf,
	"PSYNC":    psync,
}

// Repl Struct
type ReplNode struct {
	Addr          net.Addr
	ListeningPort int
	Capa          string
}

// repl nodes
var replNodes []ReplNode = []ReplNode{}

// Database
var db = MakeDB()

// Mutex for SETs
var SETsMu = sync.RWMutex{}

// Replicaof
var replicaHost string
var replicaPort int
var port int

// connection address
var addr net.Addr

// relipcas

var replicas []net.Conn = []net.Conn{}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// get the port from the user
	parsedPort := flag.Int("port", 6379, "port to listen on")
	// get the replicaof from the user
	replicaHostPtr := flag.String("replicaof", "*", "replicaof host")
	// flag to parse the port
	flag.Parse()
	if *parsedPort != 0 {
		port = *parsedPort
	}

	replicaHost = *replicaHostPtr

	// setup port repclica
	if len(flag.Args()) > 0 {
		replicaPort, _ = strconv.Atoi(flag.Args()[0])
	}

	if replicaHost != "*" {
		err := repl()
		if err != nil {
			log.Println("Failed to connect to replicaof host: ", err)
		}

	}

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Println("Failed to bind to port ", strconv.Itoa(port))
		os.Exit(1)
	}
	// Close the listener
	defer l.Close()
	fmt.Println("Listening on port ", strconv.Itoa(port))

	// Handle connections
	for {

		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection")
			os.Exit(1)
		}
		// Close the connection
		// defer conn.Close()
		// call the handleConnection function
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	// Listen on all interfaces on port 6379
	// defer conn.Close()

	// set the address
	addr = conn.RemoteAddr()

	for {
		// Create a new Resp object
		resp := NewResp(conn)

		// Read the value from the reader
		value, err := resp.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
		}
		// check if the value is an array
		if value.typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		// check if the array length is greater than 0
		if len(value.array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}
		// get the command
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		// switch on the command
		writer := NewWriter(conn)
		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		result := handler(args, conn)
		writer.Write(result)
	}
}

// ping command
func ping(args []Value, conn net.Conn) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

// echo command
func echo(args []Value, conn net.Conn) Value {
	if len(args) != 1 {
		return Value{typ: "string", str: ""}
	}
	return args[0]
}

// set command
func set(args []Value, conn net.Conn) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETsMu.Lock()
	db.Set(key, value)
	SETsMu.Unlock()

	if len(args) > 2 {
		if strings.ToUpper(args[2].bulk) == "PX" {
			expire, _ := strconv.Atoi(args[3].bulk)
			go time.AfterFunc(time.Duration(expire)*time.Millisecond, func() {
				SETsMu.Lock()
				db.Delete(key)
				SETsMu.Unlock()
			})
		}
	}
	// create a raw message from args
	// rawString := "*" + strconv.Itoa(len(args)) + "\r\n"
	// for _, arg := range args {
	// 	rawString += "$" + strconv.Itoa(len(arg.bulk)) + "\r\n" + arg.bulk + "\r\n"
	// }
	// fmt.Printf("Raw String: %d\n", len(rawString))

	if len(replicas) > 0 {
		go func() {
			replLock := sync.Mutex{}
			setValue := Value{typ: "bulk", bulk: "SET"}
			for _, replica := range replicas {
				replLock.Lock()
				rawMsg := NewWriter(replica)
				err := rawMsg.Write(Value{typ: "array", array: []Value{setValue, args[0], args[1]}})
				if err != nil {
					log.Println("Failed to write to replica: ", err)
				}
				log.Println("Sent to replica: ", string(args[1].bulk))
				replLock.Unlock()
			}
		}()
	}
	// log.Printf("Replicas: %v", replicas)

	return Value{typ: "string", str: "OK"}
}

// get command
func get(args []Value, conn net.Conn) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}
	key := args[0].bulk

	SETsMu.RLock()
	value, ok := db.Get(key)
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

// hset command
var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hset(args []Value, conn net.Conn) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

// hget command
func hget(args []Value, conn net.Conn) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

// hgetall command
func hgetall(args []Value, conn net.Conn) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	values, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "array", array: []Value{}}
	}

	var result []Value
	for k, v := range values {
		result = append(result, Value{typ: "bulk", bulk: k})
		result = append(result, Value{typ: "bulk", bulk: v})
	}

	return Value{typ: "array", array: result}
}

// info command
func info(args []Value, conn net.Conn) Value {
	// log.Println("Info Args: ", args[0].bulk)

	if len(args) == 0 && args[0].bulk != "replication" {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'info' command"}
	}

	sb := []string{}

	if replicaHost == "*" {
		sb = append(sb, "role:master")
	} else {
		sb = append(sb, "role:slave")
	}

	sb = append(sb, "master_repl_offset:0")
	sb = append(sb, "master_replid:"+"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")

	op := strings.Join(sb, "\n")
	return Value{typ: "bulk", bulk: op}
}

// Create a repl tcp connection
func repl() error {
	if replicaHost == "*" {
		return nil
	}

	go func() {
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", replicaHost, replicaPort))
		if err != nil {
			fmt.Printf("Error Occur")
		}
		log.Println("Connected to replicaof host")

		defer conn.Close()

		// send the ping command
		err = pingRequest(conn)
		if err != nil {
			fmt.Printf("Error Occur")
		}

		// REPLCONF command
		err = replConfPortRequest(conn, port)
		if err != nil {
			fmt.Printf("Error Occur")
		}

		// REPLCONF command
		err = replConfCapacityRequest(conn, "psync2")
		if err != nil {
			fmt.Printf("Error Occur")
		}

		// PSYNC command
		err = psyncRequest(conn)
		if err != nil {
			fmt.Printf("Error Occur")
		}

	}()

	return nil
}

// PSYNC command
func psync(args []Value, conn net.Conn) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'psync' command"}
	}

	if args[0].bulk != "?" {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'psync' command"}
	}

	if args[1].bulk != "-1" {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'psync' command"}
	}

	replicationID := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	// respMessage := fmt.Sprintf("FULLRESYNC %s 0\r\n", replicationID)

	var emptyRDB = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

	emptyRDBDecoded, err := hex.DecodeString(emptyRDB)

	if err != nil {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'psync' command"}
	}

	writer := NewWriter(conn)
	writer.Write(Value{typ: "bulk", bulk: fmt.Sprintf("FULLRESYNC %s 0", replicationID)})
	// response := fmt.Sprintf("$%d\r\n%s", len(emptyRDBDecoded), emptyRDBDecoded)
	// conn.Write([]byte(response))
	return Value{typ: "custom", bulk: string(emptyRDBDecoded)}
}

// replconf command
func replconf(args []Value, conn net.Conn) Value {
	// if len(args) < 2 {
	// 	return Value{typ: "error", str: "ERR wrong number of arguments for 'replconf' command"}
	// }

	// // add the replicas
	// replicas[len(replicas)] = conn
	replicas = append(replicas, conn)

	// log.Printf("replconf args: %v", args)

	// switch args[0].bulk {
	// case "listening-port":
	// 	port, _ := strconv.Atoi(args[1].bulk)
	// 	repl := getRepl()
	// 	repl.ListeningPort = port
	// case "capa":
	// 	repl := getRepl()
	// 	repl.Capa = args[1].bulk
	// default:
	// 	return Value{typ: "error", str: "ERR unsupported command"}
	// }

	return Value{typ: "string", str: "OK"}
}
