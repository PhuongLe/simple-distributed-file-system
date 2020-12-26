package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	lib "../fs533lib"
	"github.com/google/uuid"
)

var fileClientTest lib.FileClient
var fileOperation lib.FileOperation

var configurationTest lib.Configuration

//This is the overall workflow of a test
func TestMain(m *testing.M) {
	//generate log files on nodes whenever go test is called
	prepareData()

	//main testing source
	code := m.Run()

	os.Exit(code)
}

//This test is to check if put and list all commands work well
func TestSeriesOfOperations_PutAndListAllCommandsSuccessfully(t *testing.T) {
	//given
	//generate file on fs533db
	generateFile("test_file1.pa3", 10)
	generateFile("test_file2.pa3", 100)

	//when
	fileClientTest.Put("test_file1.pa3", "fs533_test_file1.pa3")
	fileClientTest.Put("test_file2.pa3", "fs533_test_file2.pa3")
	result := fileClientTest.ListAllFiles()

	//then
	if len(result) != 2 {
		t.Errorf("Got %d files while it should be 2", len(result))
	}
}

//This unit test will remove a file to check if remove functionality works well
func TestSeriesOfOperations_RemoveAndListAllCommandsSuccessfully(t *testing.T) {
	//given
	//generate and put file on fs533db
	generateFile("test_file1.pa3", 10)
	generateFile("test_file2.pa3", 100)
	fileClientTest.Put("test_file1.pa3", "fs533_test_file1.pa3")
	fileClientTest.Put("test_file2.pa3", "fs533_test_file2.pa3")

	//when
	fileClientTest.RemoveFile("fs533_test_file2.pa3")
	result := fileClientTest.ListAllFiles()

	//then
	if len(result) != 1 {
		t.Errorf("Got %d files while it should be 1", len(result))
	}
}

//This unit test will locate a specific file and return the metadata assocated with the file including the file size and the file versions
func TestSeriesOfOperations_LocateAndListAllCommandsSuccessfully(t *testing.T) {
	//given
	//generate file on fs533db
	generateFile("test_file1.pa3", 10)

	//when
	fileClientTest.Put("test_file1.pa3", "fs533_test_file1.pa3")
	fileMetadata := fileClientTest.LocateFile("fs533_test_file1.pa3")

	//then

	if fileMetadata.Fs533FileName != "fs533_test_file1.pa3" {
		t.Errorf("The filename expected is %s and the filename retrieves is: %s", "fs533_test_file1.pa3", fileMetadata.Fs533FileName)
	}

	if fileMetadata.Fs533FileVersion != 1 {
		t.Errorf("The file's version should be 1 but instead the file version that was retrieved is %d", fileMetadata.Fs533FileVersion)
	}

	if len(fileMetadata.PreferenceNodes) != 3 {
		t.Errorf("The file should have been replicated onto 3 nodes, instead it was replicated on %d nodes.", len(fileMetadata.PreferenceNodes))
	}

}

//This unit test will fetch a perticular file to see read operation functions properly
func TestSeriesOfOperations_ReadAndListAllCommandsSuccessfully(t *testing.T) {
	//given
	//generate file on fs533db
	generateFile("test_file1.pa3", 10)

	//when
	fileClientTest.Put("test_file1.pa3", "fs533_test_file1.pa3")
	fileClientTest.Fetch("fs533_test_file1.pa3", "test_new_file1.pa3")

	//then
	if !fileOperation.LocalFileExist("test_new_file1.pa3") {
		t.Errorf("Could not find file 'test_new_file1.pa3' as expected")
	}
}

//Testing series of operations: Put 2 files, Remove 1 file, Locate 1 file, Put 1 more file
func TestSeriesOfOperations_PutRemoveLocatePutAllCommandsSuccessfully(t *testing.T) {
	//Test Put First. Put 2 files
	generateFile("test_file1.pa3", 10)
	generateFile("test_file2.pa3", 100)
	fileClientTest.Put("test_file1.pa3", "fs533_test_file1.pa3")
	fileClientTest.Put("test_file2.pa3", "fs533_test_file2.pa3")

	//Then remove 1 file
	fileClientTest.RemoveFile("fs533_test_file2.pa3")
	result := fileClientTest.ListAllFiles()

	//then check if only 1 file is in the system
	if len(result) != 1 {
		t.Errorf("Got %d files while it should be 1", len(result))
	}

	//then locate that one file
	fileMetadata := fileClientTest.LocateFile("fs533_test_file1.pa3")

	//then ensure the file located is the correct one
	if fileMetadata.Fs533FileName != "fs533_test_file1.pa3" {
		t.Errorf("The filename expected is %s and the filename retrieves is: %s", "fs533_test_file1.pa3", fileMetadata.Fs533FileName)
	}

	if fileMetadata.Fs533FileVersion != 1 {
		t.Errorf("The file's version should be 1 but instead the file version that was retrieved is %d", fileMetadata.Fs533FileVersion)
	}

	if len(fileMetadata.PreferenceNodes) != 3 {
		t.Errorf("The file should have been replicated onto 3 nodes, instead it was replicated on %d nodes.", len(fileMetadata.PreferenceNodes))
	}
	//Then put one more file in the system
	fileClientTest.Put("test_file2.pa3", "fs533_test_file2.pa3")
	resultNew := fileClientTest.ListAllFiles()

	//then
	if len(resultNew) != 2 {
		t.Errorf("Got %d files while it should be 2", len(result))
	}

}

//Testing series of operations: Put 2 files, Fetch 1 file, Locate 1 file, Put 1 file
func TestSeriesOfOperations_PutReadLocateRemoveAllCommandsSuccessfully(t *testing.T) {
	//Test Put First. Put 2 files
	generateFile("test_file1.pa3", 10)
	generateFile("test_file2.pa3", 100)
	fileClientTest.Put("test_file1.pa3", "fs533_test_file1.pa3")
	fileClientTest.Put("test_file2.pa3", "fs533_test_file2.pa3")

	result := fileClientTest.ListAllFiles()

	//then ensure that 2 files exist
	if len(result) != 2 {
		t.Errorf("Got %d files while it should be 2", len(result))
	}

	//Next, fetch one of the files
	fileClientTest.Fetch("fs533_test_file1.pa3", "test_new_file1.pa3")

	//then ensure that the fetch was successful and the file was stored locally
	if !fileOperation.LocalFileExist("test_new_file1.pa3") {
		t.Errorf("Could not find file 'test_new_file1.pa3' as expected")
	}

	//then locate the file that was not fetche
	fileMetadata := fileClientTest.LocateFile("fs533_test_file2.pa3")

	//then ensure the file located is the correct one
	if fileMetadata.Fs533FileName != "fs533_test_file2.pa3" {
		t.Errorf("The filename expected is %s and the filename retrieves is: %s", "fs533_test_file2.pa3", fileMetadata.Fs533FileName)
	}

	if fileMetadata.Fs533FileVersion != 1 {
		t.Errorf("The file's version should be 1 but instead the file version that was retrieved is %d", fileMetadata.Fs533FileVersion)
	}

	if len(fileMetadata.PreferenceNodes) != 3 {
		t.Errorf("The file should have been replicated onto 3 nodes, instead it was replicated on %d nodes.", len(fileMetadata.PreferenceNodes))
	}

	//Lastly, remove 1 file
	fileClientTest.RemoveFile("fs533_test_file2.pa3")
	resultNew := fileClientTest.ListAllFiles()

	//then check if only 1 file is in the system
	if len(resultNew) != 1 {
		t.Errorf("Got %d files while it should be 1", len(resultNew))
	}

}

//Testing series of operations: Put 1 files, Fetch 1 file, Put 1 more fie, Locate 2nd file, Remove 1st file
func TestSeriesOfOperations_PutReadPutLocateRemoveAllCommandsSuccessfully(t *testing.T) {

	//Test Put First. Put 1 file
	generateFile("test_file1.pa3", 10)
	fileClientTest.Put("test_file1.pa3", "fs533_test_file1.pa3")

	result := fileClientTest.ListAllFiles()

	//then ensure that 1 files exist
	if len(result) != 1 {
		t.Errorf("Got %d files while it should be 1", len(result))
	}

	//Next, fetch the file
	fileClientTest.Fetch("fs533_test_file1.pa3", "test_new_file1.pa3")

	//then ensure that the fetch was successful and the file was stored locally
	if !fileOperation.LocalFileExist("test_new_file1.pa3") {
		t.Errorf("Could not find file 'test_new_file1.pa3' as expected")
	}

	//Put 1 more file
	generateFile("test_file2.pa3", 100)
	fileClientTest.Put("test_file2.pa3", "fs533_test_file2.pa3")

	//then locate the second file
	fileMetadata := fileClientTest.LocateFile("fs533_test_file2.pa3")

	//then ensure the file located is the correct one
	if fileMetadata.Fs533FileName != "fs533_test_file2.pa3" {
		t.Errorf("The filename expected is %s and the filename retrieves is: %s", "fs533_test_file2.pa3", fileMetadata.Fs533FileName)
	}

	if fileMetadata.Fs533FileVersion != 1 {
		t.Errorf("The file's version should be 1 but instead the file version that was retrieved is %d", fileMetadata.Fs533FileVersion)
	}

	if len(fileMetadata.PreferenceNodes) != 3 {
		t.Errorf("The file should have been replicated onto 3 nodes, instead it was replicated on %d nodes.", len(fileMetadata.PreferenceNodes))
	}

	//Then remove 1st file
	fileClientTest.RemoveFile("fs533_test_file1.pa3")
	resultNew := fileClientTest.ListAllFiles()

	//then check if only 1 file is in the system
	if len(resultNew) != 1 {
		t.Errorf("Got %d files while it should be 1", len(resultNew))
	}

}

//generate test file with input as number of random string line
func generateFile(filename string, filelen int) {
	f := fmt.Sprintf("%s/%s", configurationTest.LocalFilePath, filename)

	// check if file exists
	if _, err := os.Stat(f); err == nil {
		deleteErr := os.Remove(f)
		if deleteErr != nil {
			log.Fatal("Could not delete file: ", f)
			return
		}
	}

	//open or create a new log file with preconfigured file name
	file, err := os.OpenFile(f, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Could not open or create file ", err.Error())
		return
	}

	defer file.Close()

	//create multiple log instances
	for i := 0; i < filelen; i++ {
		appendData(file)
	}
	file.Close()
}

//Upload the testlog files to remote nodes
func prepareData() {
	configurationTest = lib.LoadConfiguration("../fs533app/config.json")
	fileClientTest.Initialize(configurationTest.GatewayNodes[0], configurationTest)
	fileClientTest.DeleteAllFiles()
	fileClientTest.ClearLocalDbTestFiles()
}

//Append a single line with random generated text data
func appendData(file *os.File) {
	rand.Seed(time.Now().UnixNano())

	//generate a random request number
	requestNo := uuid.New()

	//generate a random log message of the specified format
	logMessage := fmt.Sprintf("RandomId %s. This is a sample for unittest only...", requestNo)

	//append log instance to the log file
	file.WriteString(logMessage)
}
