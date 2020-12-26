package fs533lib

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

//FileOperation ... this struct is to handle all the local file operations. All local files are stored on ../fs533db
type FileOperation struct{}

//------------------------PUBLIC FUNCTION FOR FILE OPERATIONs----------------------------------

//LocalFileExist ... check if the file exists on local folder or not
func (a *FileOperation) LocalFileExist(fileName string) bool {
	files, err := ioutil.ReadDir("../localdb")
	if err != nil {
		log.Println("Could not open folder localdb due to err ", err)
		return true
	}

	//It will run though all log files on the log file folder
	for _, f := range files {
		if f.Name() == fileName {
			return true
		}

	}
	return false
}

//ClearFs533Db ... delete all existing files on the fs533 db folder
func (a *FileOperation) ClearFs533Db() {
	files, err := ioutil.ReadDir("../fs533db")
	if err != nil {
		log.Println("Could not open folder fs533db due to err ", err)
		return
	}

	//It will run though all log files on the log file folder
	for _, f := range files {
		err := os.Remove(a.Fs533FilePath(f.Name()))
		if err != nil {
			log.Printf("Could not delete file %s due to err %s \n", f.Name(), err)
		}
	}
}

//ClearLocalDb ... delete all existing files on the fs533 db folder
func (a *FileOperation) ClearLocalDb(prefix string) {
	files, err := ioutil.ReadDir("../localdb")
	if err != nil {
		log.Println("Could not open folder localdb due to err ", err)
		return
	}

	//It will run though all log files on the log file folder

	for _, f := range files {
		if !strings.HasPrefix(f.Name(), prefix) {
			continue
		}
		err := os.Remove(a.LocalFilePath(f.Name()))
		if err != nil {
			log.Printf("Could not delete file %s due to err %s \n", f.Name(), err)
		}
	}
}

//Copy ... copy a file to another file
func (*FileOperation) Copy(src string, des string) (int64, error) {
	srcPath := src

	sourceFileStat, err := os.Stat(srcPath)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", srcPath)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}

	defer source.Close()

	desPath := des

	destination, err := os.Create(desPath)
	if err != nil {
		return 0, err
	}
	defer destination.Close()

	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

//OpenFile ... open file for reading
func (*FileOperation) OpenFile(localfile string) *os.File {
	// check if file exists
	filepath := localfile

	// check if file exists
	file, err := os.Open(filepath)

	if err != nil {
		log.Printf("Warning: could not open file %s due to error %s \n", filepath, err)
		return nil
	}

	return file
}

//CreateOrOpenFile ... open or create a file for writing
func (*FileOperation) CreateOrOpenFile(localfile string) *os.File {
	// check if file exists
	filepath := localfile

	// check if file exists
	if _, err := os.Stat(filepath); err == nil {
		deleteErr := os.Remove(filepath)
		if deleteErr != nil {
			log.Printf("Error: could not delete exisitng file %s due to error %s \n", filepath, deleteErr)
			return nil
		}
	}

	//open or create a new log file with preconfigured file name
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Error: it is unable to create or open file %s due to error %s\n", filepath, err)
	}

	return file
}

//ListHere ... list all files on folder fs533db
func (*FileOperation) ListHere(path string) []string {
	//Collect all files on the local fs533 replicated folder
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err.Error())
	}

	var filenames []string
	for _, f := range files {
		if f.Name() == "desktop.ini" {
			continue
		}
		filenames = append(filenames, f.Name())
	}

	return filenames
}

//Delete ... delete file from folder fs533db
func (*FileOperation) Delete(fs533FileName string) {
	var err = os.Remove(fs533FileName)

	if err != nil {
		log.Printf("Error: it is unable to remove file %s due to error %s\n", fs533FileName, err.Error())
		return
	}

	log.Printf("File %s is removed\n", fs533FileName)
}

//Fs533FilePath ... the path and the file name to the full path file
func (*FileOperation) Fs533FilePath(filename string) string {
	return fmt.Sprintf("%s/%s", configuration.Fs533FilePath, filename)
}

//LocalFilePath ... the path and the file name to the full path file
func (*FileOperation) LocalFilePath(filename string) string {
	return fmt.Sprintf("%s/%s", configuration.LocalFilePath, filename)
}

//------------------------------------------------------PRIVATE FUNCTIONs------------------------------------------------
