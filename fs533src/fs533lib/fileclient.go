package fs533lib

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	fs533pb "../fs533pb"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

//FileClient ... this struct is to handle all the file operations from client side
type FileClient struct{}

//FetchedFileMetadata ... this struct is to describe all information related to a fs533file which is already fetched to local storage
type FetchedFileMetadata struct {
	Fs533FileName    string //file name on the fs533 file system
	Fs533FileVersion int32  //file version
	Fs533FileSize    int32  //file size
	LocalFileName    string //file name which is fetched on local storag
}

var replicaFiles []Fs533FileMetadata
var fetchedFiles []FetchedFileMetadata
var fileHandler FileOperation
var logManager LogManager

//-------------------------------------PUBLIC FUNCTIONS FOR FS533 FILE OPERATIONS-----------------------------------------------------

//Put ... this function is to put all files in fs533
func (a *FileClient) Put(localfilename string, fs533filename string) {
	log.Printf("************** START COMMAND PUT file '%s' to file '%s' to master node at '%s'.\n", localfilename, fs533filename, masterNode)

	startWriteTime := time.Now()

	requestID := uuid.New().String()
	result, needUserInteraction, fileVersion, preferenceNodes := a.InternalRegisterWriteFile(requestID, fs533filename, masterNode)
	if !result {
		log.Printf("Error: it is unable to put file %s as fs533 file %s.\n", localfilename, fs533filename)
		return
	}

	if needUserInteraction {
		//ask user if he/she is okay for waiting
		log.Println("************* There is a prior write request to this file which is just executed within less than 1 minute before.")
		log.Println("************* DO YOU WANT TO CONTINUE THIS COMMAND OR CANCEL IT? y to continue, n to cancel.")
		logManager.DisableLog()

		cancelRequest := false

		c := make(chan string, 1)
		go scan(c)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()

		select {
		case <-ctx.Done():
			cancelRequest = true
			logManager.EnableLog()
			log.Println("WARNING: it is timeout while waiting for user decision, put request is cancel.")
		case userDecision := <-c:
			logManager.EnableLog()
			if !strings.EqualFold(userDecision, "yes") && !strings.EqualFold(userDecision, "y") {
				cancelRequest = true
			}
		}

		if cancelRequest {
			a.InternalEndWriteFile(requestID, fs533filename, 0, masterNode, true, false)
			log.Printf("ROLLBACK COMMAND PUT file %s since it is timeout.\n", fs533filename)
			return
		}
	}

	log.Printf("Send PREPARATION WRITE request to upload local file '%s' to replica nodes at '%s'.\n", localfilename, preferenceNodes)

	numberOfAcksReceived := 0
	numberOfWaitingAcks := 0
	c := make(chan int32)

	var fs533FileSize int32
	for _, preferenceNode := range preferenceNodes {
		log.Printf("sending preparation write request to preference node '%s'", preferenceNode)

		numberOfWaitingAcks++
		go sendWritePreparationToReplica(requestID, localfilename, fs533filename, fileVersion, preferenceNode, c)
	}

	log.Printf("waiting for '%d' acknowledgement from preference nodes \n", numberOfWaitingAcks)

	reachWriteQuorumFlag := false
	for i := 0; i < numberOfWaitingAcks; i++ {
		fs533FileSize = <-c
		if fs533FileSize > 0 {
			numberOfAcksReceived++
		}
		if reachWriteQuorum(numberOfAcksReceived) {
			reachWriteQuorumFlag = true
			break
		}
	}

	//if preparation step is done by reaching write quorum, it will start asking master to commit the write transaction
	if reachWriteQuorumFlag {
		log.Printf("Send commit request to master to finish write request since it already reaches write quorum")
		result := a.InternalEndWriteFile(requestID, fs533filename, fs533FileSize, masterNode, false, true)
		if result {
			reportWriteTime(fs533filename, fileVersion, fs533FileSize, startWriteTime, time.Now())
			log.Printf("FINISH COMMAND PUT file %s, new version is %d, replicas are on nodes: %s.\n", fs533filename, fileVersion, preferenceNodes)
		} else {
			log.Printf("ROLLBACK COMMAND PUT file %s, it is unable to put file to fs533 system due to some reasons.\n", fs533filename)
		}
	} else {
		//Otherwise, it will ask master to abort the write transaction
		log.Printf("Send abort request to master to finish write request since it could not reache write quorum")
		a.InternalEndWriteFile(requestID, fs533filename, fs533FileSize, masterNode, false, false)
		log.Printf("ROLLBACK COMMAND PUT file %s, it is unable to put file to fs533 system due to some reasons.\n", fs533filename)
	}
}

//Fetch ... this function is to list all files in fs533
func (a *FileClient) Fetch(fs533filename string, localfilename string) {
	startReadTime := time.Now()
	log.Printf("************** START COMMAND FETCH file '%s': request sent to master node at '%s'.\n", fs533filename, masterNode)

	log.Printf("Send read acquire request for file '%s'. Request sent to master node at at '%s'.\n", fs533filename, masterNode)

	result, fileVersion, preferenceNodes := a.InternalReadAcquireFile(fs533filename, masterNode)
	if !result {
		log.Printf("************** FINISH COMMAND FETCH file %s, it is unable to fetch to local disk as file %s.\n", fs533filename, localfilename)
		return
	}

	if len(preferenceNodes) == 0 {
		log.Printf("File fs533 '%s' is not found on any replica.\n", fs533filename)
	} else {
		log.Printf("Receive read acquire response for file %s. File has version '%d' and is available at '%s'.\n", fs533filename, fileVersion, preferenceNodes)
	}

	fetchedFileExisted, fetchedFileSize := lookupLocalFetchedFile(fs533filename, fileVersion, localfilename)
	if fetchedFileExisted {
		log.Printf("File fs533 '%s' already cached on local disk, it is fetch to local disk as file %s.\n", fs533filename, localfilename)
		reportReadTime(fs533filename, fileVersion, fetchedFileSize, startReadTime, time.Now())
		return
	}

	replicatedFileExisted, replicatedFileSize := lookupReplicatedFile(fs533filename, preferenceNodes, fileVersion, localfilename)

	if replicatedFileExisted {
		log.Printf("File fs533 '%s' already has a replica on local disk, it is fetch to local disk as file %s.\n", fs533filename, localfilename)
		reportReadTime(fs533filename, fileVersion, replicatedFileSize, startReadTime, time.Now())
		return
	}

	log.Printf("Send read request for file %s from node '%s' to replica node at '%s'.\n", fs533filename, configuration.IPAddress, masterNode)

	var fileSize int32
	for _, replicaNode := range preferenceNodes {
		fileSize = readFileFromReplica(fs533filename, localfilename, fileVersion, replicaNode)
		if fileSize > 0 {
			log.Printf("************** FINISH COMMAND FETCH file %s, it is fetch to local disk as file %s.\n", fs533filename, localfilename)
			reportReadTime(fs533filename, fileVersion, fileSize, startReadTime, time.Now())
			break
		}

	}

	log.Printf("************** FINISH COMMAND FETCH file %s, it is unable to fetch to local disk as file %s.\n", fs533filename, localfilename)
}

//Report ... this function is to list all the local fetched files, local replicated files and global files metadata
func (a *FileClient) Report() {
	log.Println("----------------------------------FS533 REPORT-------------------------------------")
	if masterNode == configuration.IPAddress {
		log.Println("This is MASTER NODE")
	} else {
		log.Println("Master node is at ", masterNode)
	}

	log.Printf("Election term = '%d'.\n", electionTerm)

	log.Printf("All replica nodes for fs533 systems are listed below.\n")
	for idx, fs533node := range fs533nodes {
		log.Printf("Node %d: '%s', total size '%d', storing replicated files %s.\n", idx, fs533node.NodeAddress, fs533node.TotalFileSize, fs533node.Fs533files)
	}

	log.Printf("All files on fs533 systems are listed below.\n")
	for idx, fs533FileMetada := range fs533files {
		log.Printf("File %d: %s, version %d, replicated on %s.\n", idx, fs533FileMetada.Fs533FileName, fs533FileMetada.Fs533FileVersion, fs533FileMetada.PreferenceNodes)
	}

	log.Printf("All local replicated files are listed below.\n")
	for idx, replicatedFileMetada := range replicaFiles {
		log.Printf("File %d: %s, version %d.\n", idx, replicatedFileMetada.Fs533FileName, replicatedFileMetada.Fs533FileVersion)
	}

	log.Printf("All local fetched files are listed below.\n")
	for idx, fetchedFileMetada := range fetchedFiles {
		log.Printf("File %d: %s, version %d, fetched as file %s.\n", idx, fetchedFileMetada.Fs533FileName, fetchedFileMetada.Fs533FileVersion, fetchedFileMetada.LocalFileName)
	}

	log.Println("----------------------------------END REPORT-------------------------------------")
}

//ListHere ... this function is to list all files in fs533
func (a *FileClient) ListHere() {
	log.Printf("************** START COMMAND LSHERE.\n")

	log.Printf("All local replicated files on folder %s are: \n", configuration.Fs533FilePath)
	filenames := fileHandler.ListHere(configuration.Fs533FilePath)
	for idx, file := range filenames {
		log.Printf("File %d: %s\n", idx, file)
	}

	log.Printf("All local files on folder %s are: \n", configuration.LocalFilePath)
	localFilenames := fileHandler.ListHere(configuration.LocalFilePath)
	for idx, file := range localFilenames {
		log.Printf("File %d: %s\n", idx, file)
	}
}

//RemoveFile ... this public function is to delete file fs533filename from fs533
func (a *FileClient) RemoveFile(fs533filename string) {
	log.Printf("************** START COMMAND REMOVE %s: request sent from node '%s' to master node at '%s'.\n", fs533filename, configuration.IPAddress, masterNode)

	a.InternalRemoveFile(fs533filename, masterNode)

	log.Printf("************** END COMMAND REMOVE %s: Successfully removed file.\n", fs533filename)
}

//PrintLocateFile ... this function is to list all machines (name / id / IP address) of the servers that contain a copy of the file.
func (a *FileClient) PrintLocateFile(fs533filename string) {
	locatedFile := a.LocateFile(fs533filename)

	if locatedFile.Fs533FileName == "" {
		log.Printf("Warning: the file '%s' is not found on fs533 system", fs533filename)
		return
	}

	log.Printf("File '%s' is stored on following nodes \n", fs533filename)
	for idx, replica := range locatedFile.PreferenceNodes {
		log.Printf("Node %d: %s\n", idx, replica)
	}
}

//LocateFile ... this function is to locate replica node for fs533 file
func (a *FileClient) LocateFile(fs533filename string) Fs533FileMetadata {
	log.Printf("************** START COMMAND LOCATE for file '%s': request sent to master node at '%s'.\n", fs533filename, masterNode)

	//gRPC call
	locateRequest := &fs533pb.LocateRequest{
		Fs533Filename: fs533filename,
	}

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", masterNode, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s due to error: %v\n", masterNode, err.Error())
		log.Printf("Cannot query all files. Error: %s\n", errorMessage)
		return Fs533FileMetadata{}
	}

	defer cc.Close()

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewFileServiceClient(cc)

	res, err1 := c.Locate(ctx, locateRequest)

	if err1 != nil {
		log.Println("Error: It is unable read the response for listall command due to error ", err1)
		return Fs533FileMetadata{}
	}

	replicas := []string{}
	for _, replica := range res.GetReplicas() {
		replicas = append(replicas, replica.GetNodebyAddress())
	}

	returnValue := Fs533FileMetadata{
		Fs533FileName:    res.GetFs533Filename(),
		Fs533FileSize:    res.GetFs533FileSize(),
		Fs533FileVersion: res.GetFs533FileVersion(),
		PreferenceNodes:  replicas,
	}

	log.Printf("************** END COMMAND LOCATE for file '%s'.\n", fs533filename)

	return returnValue
}

//ListAllFiles ... this function is to list all files in fs533
func (a *FileClient) ListAllFiles() []string {
	log.Printf("************** START COMMAND LIST ALL: request sent to master node at '%s'.\n", masterNode)

	//gRPC call
	listAllRequest := &fs533pb.Empty{}

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", masterNode, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s due to error: %v\n", masterNode, err.Error())
		log.Printf("Cannot query all files. Error: %s\n", errorMessage)
		return []string{}
	}

	defer cc.Close()

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewFileServiceClient(cc)

	res, err1 := c.ListAll(ctx, listAllRequest)

	if err1 != nil {
		log.Println("Error: It is unable read the response for listall command due to error ", err)
		return []string{}
	}

	allfiles := []string{}

	for _, file := range res.GetFiles() {
		fileMetadata := Fs533FileMetadata{
			Fs533FileName:    file.GetFs533Filename(),
			Fs533FileSize:    file.GetFs533FileSize(),
			Fs533FileVersion: file.GetFs533FileVersion(),
		}
		allfiles = append(allfiles, fileMetadata.Fs533FileName)
	}
	return allfiles
}

//ClearLocalDbTestFiles ... This function is to delete all local test files
func (a *FileClient) ClearLocalDbTestFiles() {
	fileHandler.ClearLocalDb("test_")
}

//DeleteAllFiles ... This function is to delete all files
func (a *FileClient) DeleteAllFiles() {
	log.Printf("************** START COMMAND DELETE ALL FILES: request sent to master node at '%s'.\n", masterNode)

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", masterNode, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s due to error: %v\n", masterNode, err.Error())
		log.Printf("Cannot query all files. Error: %s\n", errorMessage)
	}

	defer cc.Close()

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewFileServiceClient(cc)

	_, err1 := c.DeleteAll(ctx, &fs533pb.Empty{})

	if err1 != nil {
		log.Println("Error: It is unable read the response for DeleteAllFiles command due to error ", err1)
	}
}

//PrintAllFiles ... this function is to print all files queried from ListAllFile API
func (a *FileClient) PrintAllFiles(files []string) {
	if len(files) == 0 {
		log.Printf("Fs533 is empty...\n")
		return
	}

	log.Printf("There are %d files on the fs533 system.\n", len(fs533files))
	for idx, file := range files {
		log.Printf("File %d: name = %s          \n", idx, file)
	}
}

//-------------------------------INTERNAL FUNCTION WHICH COULD BE REUSED ON BOTH CLIENT AND SERVER SIDE-----------------------

//Initialize ... setup fileClient with a given configuation file
func (a *FileClient) Initialize(mn string, config Configuration) {
	masterNode = mn
	configuration = config
}

//InternalEndWriteFile ... this function is to finish a write transaction, it will be commit or aborted
//arguments
//-fs533filename: filename on fs533 system which is being processed
//-targetNode: the node receives this commit request
//-cancelWriteRegistration: cancel pending write registration on master node
//-commit: the transation will be commited if it is true. Otherwise, the transaction will be aborted
func (a *FileClient) InternalEndWriteFile(requestID string, fs533filename string, fs533filesize int32, targetNodeAddress string, cancelWriteRegistration bool, commit bool) bool {
	//gRPC call
	writeEndRequest := &fs533pb.WriteEndRequest{
		RequestId:               requestID,
		Fs533Filename:           fs533filename,
		FileSize:                fs533filesize,
		CancelWriteRegistration: cancelWriteRegistration,
		Commit:                  commit,
	}

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", targetNodeAddress, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s due to error: %v\n", targetNodeAddress, err.Error())
		log.Printf("Cannot end write request for file '%s'. Error: %s\n", fs533filename, errorMessage)
		return false
	}

	defer cc.Close()

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout*100) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewFileServiceClient(cc)

	_, err1 := c.WriteEnd(ctx, writeEndRequest)

	if err1 != nil {
		log.Println("Error: It is unable ending the write transaction for file ", fs533filename, " due to error ", err1)
		return false
	}

	return true
}

//InternalRegisterWriteFile ... this function is to register write operation to master
//it should return register status, needUserInteraction, fileVersion, preferenceNodes
func (a *FileClient) InternalRegisterWriteFile(requestID string, fs533filename string, targetNodeAddress string) (bool, bool, int32, []string) {
	log.Printf("Send register write request for file %s to master node at '%s'.\n", fs533filename, masterNode)

	//gRPC call
	writeRegisterRequest := &fs533pb.WriteRegisterRequest{
		Fs533Filename: fs533filename,
		RequestId:     requestID,
	}

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", targetNodeAddress, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s due to error: %v\n", targetNodeAddress, err.Error())
		log.Printf("Cannot register write request for file '%s'. Error: %s\n", fs533filename, errorMessage)
		return false, false, -1, nil
	}

	defer cc.Close()

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewFileServiceClient(cc)

	writeRegisterResponse, err := c.WriteRegister(ctx, writeRegisterRequest)

	if err != nil {
		log.Println("Error: It is unable register a write for file ", fs533filename, " due to error ", err)
		return false, false, -1, nil
	}

	log.Printf("Receive register write response for file %s: version %d, preference nodes are '%s', need user interaction is %v.\n", fs533filename, writeRegisterResponse.GetFileVersion(), writeRegisterResponse.GetPreferenceNodes(), writeRegisterResponse.GetNeedUserInteraction())

	return true, writeRegisterResponse.GetNeedUserInteraction(), writeRegisterResponse.GetFileVersion(), writeRegisterResponse.GetPreferenceNodes()
}

//InternalReadAcquireFile ... this function is to send read acquire request to the target node to get the file's latest version
//It return file's latest version on the target replica node and its preference nodes
func (a *FileClient) InternalReadAcquireFile(fs533filename string, replicaNodeAddress string) (bool, int32, []string) {
	//gRPC call
	readAcquireRequest := &fs533pb.ReadAcquireRequest{
		SenderAddress: configuration.IPAddress,
		Fs533Filename: fs533filename,
	}

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", replicaNodeAddress, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s due to error: %v\n", replicaNodeAddress, err.Error())
		log.Printf("Cannot acquire a read for file '%s'. Error: %s\n", fs533filename, errorMessage)
		return false, -1, nil
	}

	defer cc.Close()

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewFileServiceClient(cc)

	readAcquireResponse, err := c.ReadAcquire(ctx, readAcquireRequest)

	if err != nil {
		log.Println("Error: It is unable acquire read file ", fs533filename, " due to error ", err)
		return false, -1, nil
	}

	if !readAcquireResponse.Status {
		log.Println("Error: It is unable acquire read file ", fs533filename)
		return false, -1, nil
	}

	return true, readAcquireResponse.GetFileVersion(), readAcquireResponse.GetPreferenceNodes()
}

//InternalRemoveFile ... this function is to send remove request directly to the target node
func (a *FileClient) InternalRemoveFile(fs533filename string, targetNode string) bool {
	//gRPC call
	removeRequest := &fs533pb.DeleteRequest{
		SenderAddress: configuration.IPAddress,
		Fs533Filename: fs533filename,
	}

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", targetNode, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to node at %s due to error: %v\n", masterNode, err.Error())
		log.Printf("Cannot remove file '%s'. Error: %s\n", fs533filename, errorMessage)
		return false
	}

	defer cc.Close()

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewFileServiceClient(cc)

	removeResponse, err := c.Delete(ctx, removeRequest)

	if err != nil {
		log.Println("Error: It is unable delete file ", fs533filename, " due to error ", err)
		return false
	}

	if !removeResponse.Status {
		log.Println("Error: It is unable delete file ", fs533filename)
		return false
	}

	return true
}

//--------------------------------------------------------PRIVATE FUNCTIONs---------------------------------------------------

func reportWriteTime(fs533filename string, fileVersion int32, fileSize int32, startWriteTime time.Time, endWriteTime time.Time) {
	log.Println(" ")
	log.Printf("******************* WRITE (INSERT OR UPDATE) REPORT **********************")
	log.Printf("Report for file '%s', version '%d', size '%d'", fs533filename, fileVersion, fileSize)
	log.Printf("Start write is at '%s'", ConvertTimeToLongString(startWriteTime))
	log.Printf("End write is at '%s'", ConvertTimeToLongString(endWriteTime))
	log.Printf("Different is '%d' milliseconds", endWriteTime.Sub(startWriteTime).Milliseconds())
	log.Printf("******************* END WRITE REPORT **********************")
	log.Println(" ")
}

func reportReadTime(fs533filename string, fileVersion int32, fileSize int32, startWriteTime time.Time, endWriteTime time.Time) {
	log.Println(" ")
	log.Printf("******************* READ REPORT **********************")
	log.Printf("Report for file '%s', version '%d', size '%d'", fs533filename, fileVersion, fileSize)
	log.Printf("Start read is at '%s'", ConvertTimeToLongString(startWriteTime))
	log.Printf("End read is at '%s'", ConvertTimeToLongString(endWriteTime))
	log.Printf("Different is '%d' milliseconds", endWriteTime.Sub(startWriteTime).Milliseconds())
	log.Printf("******************* END READ REPORT **********************")
	log.Println(" ")
}

//send write preparation to replica node
func sendWritePreparationToReplica(requestID string, localfilename string, fs533filename string, fileVersion int32, preferenceNode string, ackChan chan int32) {
	//if preferenceNode is the current node then just simply clone local file to fs533 file
	if preferenceNode == configuration.IPAddress {
		log.Printf("Perference node '%s' is the current node so it will just simply copy it to the fs533db folder", preferenceNode)
		tempfilename := fmt.Sprintf("___tempfile_%s", uuid.New())
		fs, err := fileHandler.Copy(fileHandler.LocalFilePath(localfilename), fileHandler.Fs533FilePath(tempfilename))
		if err == nil {

			temporaryFile := TemporaryFs533File{
				Fs533FileName:     fs533filename,
				Fs533FileVersion:  fileVersion,
				RequestID:         requestID,
				TemporaryFileName: tempfilename,
				Fs533FileSize:     int32(fs),
			}
			ackChan <- int32(fs)
			temporaryFiles = append(temporaryFiles, temporaryFile)
		}
		return
	}

	//open file for reading
	file := fileHandler.OpenFile(fileHandler.LocalFilePath(localfilename))
	if file == nil {
		log.Println("Could not open local file % for pushing to replica node ", localfilename)
		ackChan <- 0
		return
	}

	defer file.Close()

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", preferenceNode, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s due to error: %v\n", preferenceNode, err.Error())
		log.Printf("Cannot proceed write preparation request for file '%s'. Error: %s\n", fs533filename, errorMessage)
		ackChan <- 0
		return
	}

	defer cc.Close()

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout*100) * time.Second
	_, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewFileServiceClient(cc)

	stream, err := c.WritePrepare(context.Background())
	if err != nil {
		log.Printf("error while calling WritePrepare: %v", err)
		ackChan <- 0
		return
	}

	//Read file in a buf of chunk size and then stream it back to client
	buf := make([]byte, configuration.FileChunkSize)

	writing := true
	for writing {
		// put as many bytes as `chunkSize` into the buf array.
		_, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		writePreparationRequest := &fs533pb.WritePreparationRequest{
			Fs533Filename:  fs533filename,
			Fs533Filevalue: buf,
			FileVersion:    fileVersion,
			RequestId:      requestID,
		}
		stream.Send(
			writePreparationRequest)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("error while receiving response from write preparation request: %v", err)
		ackChan <- 0
		return
	}

	ackChan <- res.GetFileSize()
}

//look up replicated file on the fs533 file db on the host
func lookupReplicatedFile(fs533filename string, preferenceNodes []string, fileVersion int32, localfilename string) (bool, int32) {
	isReplicaNode := false
	for _, preferenceNode := range preferenceNodes {
		if preferenceNode == configuration.IPAddress {
			isReplicaNode = true
			break
		}
	}

	if !isReplicaNode {
		return false, 0
	}

	hasReplicaFileOnLocalStorage := false
	hasCorrectReplicaVersionOnLocalStorage := false
	for _, replicatedFile := range replicaFiles {
		if replicatedFile.Fs533FileName == fs533filename {
			hasReplicaFileOnLocalStorage = true
			if replicatedFile.Fs533FileVersion == int32(fileVersion) {
				hasCorrectReplicaVersionOnLocalStorage = true
			}
			break
		}
	}

	if !hasCorrectReplicaVersionOnLocalStorage {
		if hasReplicaFileOnLocalStorage {
			log.Printf("Warning: this node has a replica on local storage but its version is out of date")
		} else {
			log.Printf("Warning: this node does not have a replica on local storage")
		}
		return false, 0
	}

	//copy the replica to the new local file name
	fs, err := fileHandler.Copy(fileHandler.Fs533FilePath(fs533filename), fileHandler.LocalFilePath(localfilename))
	if err != nil {
		log.Printf("Warning: could not copy the replica to the local file")
		return false, 0
	}

	//Finally, copy the new copy to fetchedfiles
	newFetchedFile := FetchedFileMetadata{
		Fs533FileName:    fs533filename,
		Fs533FileVersion: fileVersion,
		LocalFileName:    localfilename,
		Fs533FileSize:    int32(fs),
	}

	fetchedFiles = append(fetchedFiles, newFetchedFile)
	return true, newFetchedFile.Fs533FileSize
}

//this function is to check if there is already a fetched file for this fs533filename with same version or not.
func lookupLocalFetchedFile(fs533filename string, fileVersion int32, localfilename string) (bool, int32) {
	var fetchedFilesWithSameVersion []FetchedFileMetadata
	var identicalLocalFetechedFile FetchedFileMetadata

	for _, ff := range fetchedFiles {
		if ff.Fs533FileName == fs533filename && ff.Fs533FileVersion == fileVersion {
			if ff.LocalFileName == localfilename {
				identicalLocalFetechedFile = ff
				break
			}
			fetchedFilesWithSameVersion = append(fetchedFilesWithSameVersion, ff)
		}
	}

	//if there is already a copy of that file then just simply return it
	if identicalLocalFetechedFile.Fs533FileName != "" {
		return true, identicalLocalFetechedFile.Fs533FileSize
	}

	if len(fetchedFilesWithSameVersion) == 0 {
		return false, 0
	}

	//Otherwise, if there is a fetched file with same version but with different name, then copy that fetched file to a new file named localfilename
	firstFetchedFile := fetchedFilesWithSameVersion[0]
	fs, err := fileHandler.Copy(firstFetchedFile.LocalFileName, localfilename)
	if err != nil {
		log.Printf("Unable to copy file %s to file %s on local storage", firstFetchedFile.LocalFileName, localfilename)
		return false, 0
	}

	//Finally, copy the new copy to fetchedfiles
	newFetchedFile := FetchedFileMetadata{
		Fs533FileName:    fs533filename,
		Fs533FileVersion: fileVersion,
		LocalFileName:    localfilename,
		Fs533FileSize:    int32(fs),
	}

	fetchedFiles = append(fetchedFiles, newFetchedFile)
	return true, newFetchedFile.Fs533FileSize
}

func readFileFromReplica(fs533filename string, localfilename string, fileVersion int32, replicaNode string) int32 {
	//gRPC call
	readRequest := &fs533pb.ReadRequest{
		Fs533Filename: fs533filename,
		FileVersion:   int32(fileVersion),
	}

	log.Printf("Send read request for file %s to a replica node at '%s'.\n", fs533filename, replicaNode)

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", replicaNode, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to replica node at %s due to error: %v\n", replicaNode, err.Error())
		log.Printf("Cannot remove file '%s'. Error: %s\n", fs533filename, errorMessage)
		return 0
	}

	defer cc.Close()

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewFileServiceClient(cc)

	resStream, err := c.Read(ctx, readRequest)

	if err != nil {
		log.Println("Error: It is unable acquire read file ", fs533filename, " due to error ", err)
		return 0
	}

	file := fileHandler.CreateOrOpenFile(fileHandler.LocalFilePath(localfilename))
	defer file.Close()

	//Loop to received the stream data response from the log agent
	fileSize := 0
	log.Print("read in progress...")
	counting := 0
	for {
		msg, err := resStream.Recv()
		if err == io.EOF {
			break
		}

		//if there is an error thrown while streaming data, it should be logged
		if err != nil {
			log.Printf("Error while reading stream: %v\n", err.Error())
			return 0
		}

		//successfully read a data chunk from the streaming response
		nb, err := file.Write(msg.GetFs533Filevalue())
		if err != nil {
			log.Printf("Error while writing to file: %v\n", err.Error())
		}

		fileSize = fileSize + nb
		if counting%10000 == 0 {
			fmt.Print(".")
		}
		counting++
		//log.Printf("Write '%d' byte from replica '%s' to file '%s' \n", nb, replicaNode, localfilename)
	}
	log.Printf("Write '%d' byte from replica '%s' to file '%s' \n", fileSize, replicaNode, localfilename)
	//Finally, copy the new copy to fetchedfiles
	newFetchedFile := FetchedFileMetadata{
		Fs533FileName:    fs533filename,
		Fs533FileVersion: fileVersion,
		LocalFileName:    localfilename,
		Fs533FileSize:    int32(fileSize),
	}

	fetchedFiles = append(fetchedFiles, newFetchedFile)

	return int32(fileSize)
}

//scan for user interaction response
func scan(in chan string) {
	var input string
	_, err := fmt.Scanln(&input)
	if err != nil {
		panic(err)
	}

	in <- input
}
