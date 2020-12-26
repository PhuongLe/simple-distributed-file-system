package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	lib "../fs533lib"
)

var configuration lib.Configuration
var membership lib.MembershipService
var fileClient lib.FileClient
var fileserver lib.FileServer
var logmanager lib.LogManager
var scanner *bufio.Scanner
var mw io.Writer

//main function to handle commands such as init, join, leave, put, ls, etc
func main() {
	logmanager.EnableLog()

	configuration = lib.LoadConfiguration("../fs533app/config.json")

	membership.SetConfiguration(configuration)

	//process user commands such as init group, join/leave group, report or file operation
	processUserCommands()
}

//process first user command, it could be initialize a new membership service or join an existing one. It will then loop to acquire the next commands
func processUserCommands() {
	init := flag.Bool("i", false, "init group")
	guest := flag.Bool("g", false, "a guest")

	flag.Parse()

	startFs533Service(*init, *guest)
	scanner = bufio.NewScanner(os.Stdin)

	for {
		if !*guest {
			scanner.Scan()
			nextOption := scanner.Text()

			if nextOption != "c" {
				break
			}
			//time.Sleep(2 * time.Second)
		}
		exit := acquireNextCommand(*guest)
		if exit {
			break
		}
	}
}

//start fs533 file system service
//Arguments
//-init: initialize new membership service group if it is true. Otherwise, join an existing one by sending join request to master node
func startFs533Service(init bool, guest bool) {
	if !guest {
		membership.Start(init)
	} else {
		fileClient.Initialize(configuration.GatewayNodes[0], configuration)
	}
}

func stopFs533Service() {
	membership.Stop()
}

//asking user command after being a member of a group, aka after start fs533 file system service
//the next commands can be: stop fs533 service, report, file operations
//returns true if the program should exist. Otherwise, return false
func acquireNextCommand(guest bool) bool {

	if !guest {
		if membership.Len() == 0 {
			fmt.Println("Fs533 file system is stopped. Do you want to start again? yes | no")
		} else {
			fmt.Println("Fs533 file system is started. What do you want to do next? stop | report | file operations")
		}
	} else {
		fmt.Println("which operation do you want to do now?")
	}

	scanner.Scan()
	nextOption := scanner.Text()

	if !guest {
		if membership.Len() == 0 {
			if strings.EqualFold(nextOption, "yes") || strings.EqualFold(nextOption, "y") {
				startFs533Service(false, false)
				return false
			}
			fmt.Println("okay, see you! bye...")
			return true
		}
	}

	switch nextOption {
	case "exit":
		fmt.Printf("okay, see you! bye...\n")
		return true
	case "stop":
		stopFs533Service()
		return false
	case "report":
		if !guest {
			membership.Report()
		}
		fileClient.Report()
		return false
	case "ls":
		fileClient.PrintAllFiles(fileClient.ListAllFiles())
		return false
	case "lshere":
		fileClient.ListHere()
		return false
	default:
		nextOptionsParams := strings.Split(nextOption, " ")
		switch nextOptionsParams[0] {
		case "put":
			if len(nextOptionsParams) != 3 {
				break
			}
			localfilename := nextOptionsParams[1]
			fs533filename := nextOptionsParams[2]

			//call put operation handler
			fileClient.Put(localfilename, fs533filename)
			return false
		case "get":
			if len(nextOptionsParams) != 3 {
				break
			}
			localfilename := nextOptionsParams[2]
			fs533filename := nextOptionsParams[1]

			//call get operation handler
			fileClient.Fetch(fs533filename, localfilename)
			return false
		case "remove":
			if len(nextOptionsParams) != 2 {
				break
			}
			fs533filename := nextOptionsParams[1]

			fileClient.RemoveFile(fs533filename)
			return false

		case "locate":
			if len(nextOptionsParams) != 2 {
				break
			}
			fs533filename := nextOptionsParams[1]

			fileClient.PrintLocateFile(fs533filename)
			return false

		default:
			fmt.Printf("the option %s is not recognized, please try again...\n", nextOption)
			return false
		}
	}

	return true
}
