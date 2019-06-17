package main

import (
	"flag"
	"log"
	"strings"

	"github.com/go-redis/redis"
	"github.com/tidwall/redcon"
)

var (
	flagHost    = flag.String("h", ":6399", "host address")
	flagCluster = flag.String("dest", "", "destination redis cluster (seperator ',') example:\"127.0.0.1:6379,127.0.0.2:6379\"")
)

func main() {
	flag.Parse()
	go log.Printf("started server at %s", *flagHost)
	addrs := strings.Split(*flagCluster, ",")
	destRedis := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addrs})
	err := redcon.ListenAndServe(*flagHost,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "mget":
				if len(cmd.Args) < 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}

				pipe := destRedis.Pipeline()

				do := make([]*redis.Cmd, len(cmd.Args)-1)
				for i := 1; i < len(cmd.Args); i++ {
					do[i-1] = pipe.Do("get", cmd.Args[i])

				}

				cmd, _ := pipe.Exec()

				conn.WriteArray(len(cmd))

				for i := 0; i < len(cmd); i++ {

					if do[i].Err() == redis.Nil {
						conn.WriteString("(nil)")
						continue
					}
					if do[i].Err() != nil {
						conn.WriteError(do[i].Err().Error())
						continue
					}

					str, _ := do[i].String()
					conn.WriteString(str)

				}
			case "mset":
				if len(cmd.Args) < 3 || (len(cmd.Args)-1)%2 != 0 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}

				pipe := destRedis.Pipeline()

				for i := 1; i < len(cmd.Args); i = i + 2 {
					pipe.Do("set", cmd.Args[i], cmd.Args[i+1])
				}

				cmd, _ := pipe.Exec()

				for _, c := range cmd {
					if c.Err() != nil {
						conn.WriteError(c.Err().Error())
						return
					}
				}

				conn.WriteString("OK")
			}
		},
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
