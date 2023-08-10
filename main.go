package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	datastore "yoti-test/dataStore"
	"yoti-test/persistence"
)

func main() {
	persistenceConfig := persistence.NewPersistence()
	kvStore := datastore.NewKeyValueStore(persistenceConfig)

	fmt.Println("Simple Key-Value Store CLI in Go")
	fmt.Println("--------------------------------")
	fmt.Println("Available commands:")
	fmt.Println("SET key value")
	fmt.Println("GET key")
	fmt.Println("DELETE key")
	fmt.Println("FLUSHDB")
	fmt.Println("EXIT")
	fmt.Println("--------------------------------")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("127.0.0.1:6379> ")
		scanner.Scan()
		input := strings.TrimSpace(scanner.Text())
		parts := strings.Split(input, " ")

		if len(input) == 0 {
			continue
		}

		if len(parts) == 0 {
			fmt.Println("Invalid input")
			continue
		}

		command := strings.ToUpper(parts[0])
		switch command {
		case "SET":
			if len(parts) < 3 {
				fmt.Printf("(error) ERR unknown command '%v'\n", command)
				continue
			}
			key := parts[1]
			value := parts[2]
			kvStore.Set(key, value)
			fmt.Println("OK")
		case "GET":
			if len(parts) < 2 {
				fmt.Printf("(error) ERR unknown command '%v'\n", command)
				continue
			}
			key := parts[1]
			value, exists := kvStore.Get(key)
			if exists {
				fmt.Printf("'%v'\n", value)
			} else {
				fmt.Println("(nil)")
			}
		case "DELETE":
			if len(parts) < 2 {
				fmt.Println("Usage: DELETE key")
				continue
			}
			key := parts[1]
			kvStore.Delete(key)
			fmt.Println("OK")
		case "FLUSHDB":
			if len(parts) > 1 {
				fmt.Printf("(error) ERR unknown command '%v'\n", command)
				continue
			}
			kvStore.FlushDB()
			fmt.Println("OK")
		case "EXIT":
			fmt.Println("Exiting...")
			os.Exit(0)
		default:
			fmt.Printf("(error) ERR unknown command '%v'\n", command)
		}
	}
}
