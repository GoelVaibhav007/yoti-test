package persistence

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

type Persistence struct {
	fileName         string
	fileWriter       *os.File
	separator        string
	deletedSeparator string
}

func NewPersistence() *Persistence {
	pwd, _ := os.Getwd()
	return &Persistence{
		fileName:         fmt.Sprintf("%s/data/%s", pwd, "aof.txt"),
		separator:        "====>",
		deletedSeparator: "---->",
	}
}

func (p *Persistence) StartPersistence() {
	file, err := os.OpenFile(p.fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}

	p.fileWriter = file
}

func (p *Persistence) PopulatePersistedStore(store map[string]string) {
	file, err := os.Open(p.fileName)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		storedData := strings.SplitN(line, p.separator, 2)
		if storedData[1] != "" {
			store[storedData[0]] = storedData[1]
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}
}

func (p *Persistence) PersistKey(key, value string) {
	data := strings.Join([]string{key, value}, p.separator) + "\n"
	_, err := p.fileWriter.Write([]byte(data))
	if err != nil {
		log.Println("error persisting data to file: ", err)
	}
}

func (p *Persistence) MarkKeyAsDeleted(key string) {
	file, err := os.Open(p.fileName)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	lines := []string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		storedData := strings.SplitN(line, p.separator, 2)
		if key == "*" || (storedData[1] != "" && storedData[0] == key) {
			line = strings.Join(storedData, p.deletedSeparator)
		}

		lines = append(lines, line)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	p.fileWriter.Close()
	for _, line := range lines {
		_, err := file.WriteString(line + "\n")
		if err != nil {
			return
		}
	}
	p.StartPersistence()
}
