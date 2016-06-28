package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/delaemon/sonyflake"
)

var (
	respError     = []byte("ERROR\r\n")
	memdSep       = []byte("\r\n")
	memdSepLen    = len(memdSep)
	memdSpace     = []byte(" ")
	memdValHeader = []byte("VALUE ")
	memdValFooter = []byte("END\r\n")
)

type MemdCmd interface {
	Execute(net.Conn) error
}

func BytesToCmd(data []byte) (cmd MemdCmd, err error) {
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
			if err = WriteError(conn); err != nil {
				log.Printf("error on write error: %s", err)
				return err
			}
			return nil
		}
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

type MemdCmdQuit int

func (cmd MemdCmdQuit) Execute(conn net.Conn) error {
	return io.EOF
}

type MemdValue struct {
	Keys   []string
	Flags  int
	Values []string
}

func (v MemdValue) WriteTo(w io.Writer) (int64, error) {
	var b bytes.Buffer
	for i, key := range v.Keys {
		b.Write(memdValHeader)
		b.WriteString(key)
		b.Write(memdSpace)
		b.WriteString(strconv.Itoa(v.Flags))
		b.Write(memdSpace)
		b.WriteString(strconv.Itoa(len(v.Values[i])))
		b.Write(memdSep)
		b.WriteString(v.Values[i])
		b.Write(memdSep)
	}
	b.Write(memdValFooter)
	return b.WriteTo(w)
}

func WriteError(conn net.Conn) (err error) {
	_, err = conn.Write(respError)
	if err != nil {
		log.Print(err)
	}
	return
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
			log.Print(err)
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
		cmd, err := BytesToCmd(scanner.Bytes())
		if err != nil {
			log.Print(err)
			if err = WriteError(conn); err != nil {
				log.Printf("error on write error: %s", err)
				return
			}
			continue
		}
		err = cmd.Execute(conn)
		if err == io.EOF {
			return
		} else if err != nil {
			log.Printf("error on write to conn: %s", err)
			return
		}
	}
}

func main() {
	listenTCP("localhost", 11211)
}
