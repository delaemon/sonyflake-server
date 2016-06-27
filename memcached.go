package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/delaemon/sonyflake"
)

var (
	memdSep    = []byte("\r\n")
	memdSepLen = len(memdSep)
	memdSpc    = []byte(" ")
)

func listenTCP(host string, port int) error {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}

	return listen(l)
}

func listen(l net.Listener) error {
	log.Printf("Listening at %s", l.Addr().String())

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Error on accept connection: %s", err)
			continue
		}
		log.Printf("Connected by %s", conn.RemoteAddr().String())

		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		cmd, err := bytesToCmd(scanner.Bytes())
		if err != nil {
			return
		}
		err = cmd.Execute(conn)
		if err == io.EOF {
			log.Printf("eof")
			return
		} else if err != nil {
			log.Printf("error on write to conn: %s", err)
			return
		}
	}
}

// MemdCmd defines a command.
type MemdCmd interface {
	Execute(net.Conn) error
}

func bytesToCmd(data []byte) (cmd MemdCmd, err error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("No command")
	}

	fields := strings.Fields(string(data))
	switch name := strings.ToUpper(fields[0]); name {
	case "GET", "GETS":
		cmd = &MemdCmdGet{
			Name: name,
			Keys: fields[1:],
		}
	case "QUIT":
		cmd = MemdCmdQuit(0)
	default:
		err = fmt.Errorf("Unknown command: %s", name)
	}
	return
}

type MemdCmdGet struct {
	Name string
	Keys []string
}

func (cmd *MemdCmdGet) Execute(conn net.Conn) error {
	values := make([]string, len(cmd.Keys))
	for i, _ := range cmd.Keys {
		id, err := sf.NextID()
		if err != nil {
			log.Printf("error on write error: %s", err)
			return err
		}
		//sonyflake.Decompose(id)
		log.Printf("Generated ID: %d", id)
		values[i] = strconv.FormatUint(id, 10)
	}
	_, err := MemdValue{
		Keys:   cmd.Keys,
		Flags:  0,
		Values: values,
	}.WriteTo(conn)
	return err
}

// MemdCmdQuit defines QUIT command.
type MemdCmdQuit int

// Execute disconnect by server.
func (cmd MemdCmdQuit) Execute(conn net.Conn) error {
	return io.EOF
}

// MemdValue defines return value for client.
type MemdValue struct {
	Keys   []string
	Flags  int
	Values []string
}

// WriteTo writes content of MemdValue to io.Writer.
// Its format is compatible to memcached protocol.
func (v MemdValue) WriteTo(w io.Writer) (int64, error) {
	var b bytes.Buffer
	for i, key := range v.Keys {
		b.WriteString(key)
		b.Write(memdSpc)
		b.WriteString(strconv.Itoa(v.Flags))
		b.Write(memdSpc)
		b.WriteString(strconv.Itoa(len(v.Values[i])))
		b.Write(memdSep)
		b.WriteString(v.Values[i])
		b.Write(memdSep)
	}
	return b.WriteTo(w)
}

var sf *sonyflake.Sonyflake

func init() {
	var st sonyflake.Settings
	st.MachineID = serverID
	sf = sonyflake.New(st)
	if sf == nil {
		panic("sonyflake not created")
	}
}

func serverID() (uint16, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		panic(err)
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			log.Fatalln(err)
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return uint16(ip[2])<<8 + uint16(ip[3]), nil
		}
	}
	return 0, err
}

func handler(w http.ResponseWriter, r *http.Request) {
	id, err := sf.NextID()
	if err != nil {
	}
	sonyflake.Decompose(id)
}

func main() {
	listenTCP("localhost", 11211)
}
