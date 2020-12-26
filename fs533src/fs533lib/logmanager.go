package fs533lib

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
)

//LogManager ... this struct is to manage all log events
type LogManager struct{}

//EnableLog ... this function is to enable logs related to file operation, it will log on both terminal and file
func (a *LogManager) EnableLog() {
	fileName := fmt.Sprintf("../fs533log/filesystem-%s.log", ConvertCurrentTimeToString())
	logFile, _ := os.Create(fileName)
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
}

//DisableLog ... this function is to disable logs
func (a *LogManager) DisableLog() {
	log.SetOutput(ioutil.Discard)
}
