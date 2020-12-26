package fs533lib

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	fs533pb "../fs533pb"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type nodeMetadata struct {
	NodeAddress   string   //ip address of a node on active members
	Fs533files    []string //fs533 files which are replicated on this node
	TotalFileSize int32    //total volumn of all files on the node
}

//Fs533FileMetadata ... this truct is to describe all the information of a file located on fs533 system
type Fs533FileMetadata struct {
	Fs533FileName    string    //file name on the fs533 file system
	Fs533FileVersion int32     //file version
	Fs533FileSize    int32     //file size
	RegisteredTime   time.Time //latest registered time
	PreferenceNodes  []string  //list of its replica nodes
}

//WriteTransaction ... this struct is to describe a pendding write transaction
type WriteTransaction struct {
	Fs533FileName    string   //the file being written
	Fs533FileVersion int32    //the new version of this file
	RequestID        string   //id of the write request which is to differentiate write transaction
	PreferenceNodes  []string //list of its replica nodes
	RegisteredTime   time.Time
}

//TemporaryFs533File ... this struct is to describe the temporary file which is created for write preparation
type TemporaryFs533File struct {
	Fs533FileName     string //the file being written
	Fs533FileVersion  int32  //the new version of this file
	RequestID         string //id of the write request which is to differentiate write transaction
	TemporaryFileName string //the file will be saved as a temporary file while it is waiting for the commit request
	Fs533FileSize     int32  //file size
}

type relocatedFileMetadata struct {
	fs533FileName    string
	fs533FileSize    int32
	fs533FileVersion int32
	srcReplicaNode   string
	dstReplicaNode   string
}

//FileServer ... this struct is to handle all the file operations from server side
type FileServer struct{}
type grpcFileServer struct{}

type readAcquireResult struct {
	status     bool
	version    int32
	targetNode string
}

var fs533files []Fs533FileMetadata
var fs533nodes []nodeMetadata
var pendingWriteTransactions []WriteTransaction
var temporaryFiles []TemporaryFs533File
var masterNode string
var fileClient FileClient

//this counter is used for tracking the order of backup requests sent from master node to all of its members
var backupCounter int32

//this is used for self-voting to be master. This value will be encreased by one for each election time
var electionTerm int32

//this is usef for self-voting to be master. This value will be changed to true while it is doing self-promoting process.
var onElectionProcess bool

//this flag is to indicate whether the voting process is done or not. true mean the election is in progress. Otherwise, it is done
//var electionIsOn bool

var voteRecords []int32

//Stop ... stop the current service by resetting all global variable
func (a *FileServer) Stop() {
	electionTerm = 0
	backupCounter = 0
	masterNode = ""
}

//Start ... start file server by initialize the global variables
func (a *FileServer) Start(master string, eTerm int32, initFs533Files []Fs533FileMetadata, initPendingWriteTransactions []WriteTransaction) {
	masterNode = master
	electionTerm = eTerm
	a.UpdateMetada(initFs533Files, initPendingWriteTransactions)
}

//UpdateMetada ... update all the information of file operations that a master node should know
func (a *FileServer) UpdateMetada(initFs533Files []Fs533FileMetadata, initPendingWriteTransactions []WriteTransaction) {
	fs533files = initFs533Files
	pendingWriteTransactions = initPendingWriteTransactions

	updateFs533nodes()
}

//StartLeaderElectionProcess ... start self voting process to be the new master node
func (a *FileServer) StartLeaderElectionProcess() {
	onElectionProcess = true

	electionAttempt := 0
	for true {
		//another node is already voted as master so this node should stop self-promoting process
		if !onElectionProcess {
			break
		}

		isDone := a.StartPromotingAsMaster()
		if isDone {
			break
		}

		//if the leader election has failed, it will wait 200 - 300 miliseconds before starting another self-voting session
		//each leader candidate will have different wait time to avoid looping self election forever

		electionAttempt++
		//parse ip address to number
		ipNumbers := strings.Split(configuration.IPAddress, ".")
		var seed int64
		for _, ipNumber := range ipNumbers {
			ipNumberVal, _ := strconv.Atoi(ipNumber)
			seed = seed + int64(ipNumberVal)
		}
		seed = seed + time.Now().UnixNano()
		s1 := rand.NewSource(seed)
		r1 := rand.New(s1)

		n := r1.Intn(10)

		waitingTime := 200 + n*100
		log.Printf("Election has failed, try again in '%d' millisecond, attempt = '%d' ... \n", waitingTime, electionAttempt)

		time.Sleep(time.Duration(waitingTime) * time.Millisecond)
	}
}

//StartPromotingAsMaster ... start sending requests to other nodes to self-promote as a new master node
func (a *FileServer) StartPromotingAsMaster() bool {
	//the voting request is to self motivate to be leader of the next term
	electionTerm = electionTerm + 1

	c := make(chan bool)

	go func() {
		isElectionSuccessful := sendVotingRequests()
		c <- isElectionSuccessful
	}()

	// Listen on our channel AND a timeout channel - which ever happens first.
	select {
	case res := <-c:
		//if the election is successful, set it as master and announce the result to all the other nodes
		if res && onElectionProcess {
			//electionIsOn = false
			masterNode = configuration.IPAddress
			onElectionProcess = false
			sendAnnouncementRequests()
			return true
		}
	case <-time.After(300 * time.Millisecond):
		fmt.Println("election is timeout but still unable to specify if it could be a leader or not. The process will be restarted in few milliseconds")
		return false
	}
	return false
}

//FireNodeLeave ... farewell a node leave by handover its replicated files to the other nodes
func (a *FileServer) FireNodeLeave(nodeleaveAddress string) {
	startFiringTime := time.Now()
	log.Printf("Start firing leaving process for node '%s'.", nodeleaveAddress)
	if nodeleaveAddress == masterNode {
		//promote another node to be master
		followers := membershipservice.GetFollowerAddresses()
		for _, member := range followers {
			go sendPromoteRequest(followers[0], member)
		}
	}

	//Rearrange replicated files on the node leave to the other nodes
	var relocatedFileNames []string
	for _, node := range fs533nodes {
		if node.NodeAddress != nodeleaveAddress {
			continue
		}
		relocatedFileNames = node.Fs533files
		break
	}

	var relocatedFiles []relocatedFileMetadata
	for _, relocatedFileName := range relocatedFileNames {
		rMeta := relocatedFileMetadata{
			fs533FileName:  relocatedFileName,
			fs533FileSize:  0,
			srcReplicaNode: "",
			dstReplicaNode: "",
		}
		for _, fs533File := range fs533files {
			if relocatedFileName == fs533File.Fs533FileName {
				rMeta.fs533FileSize = fs533File.Fs533FileSize
				rMeta.fs533FileVersion = fs533File.Fs533FileVersion
				break
			}
		}
		relocatedFiles = append(relocatedFiles, rMeta)
	}

	//remove leaving node from file's preference nodes
	for idx, fs533File := range fs533files {
		pendingIndex := -1

		for i := 0; i < len(fs533File.PreferenceNodes); i++ {
			if fs533File.PreferenceNodes[i] == nodeleaveAddress {
				pendingIndex = i
				break
			}
		}
		if pendingIndex == -1 {
			continue
		}

		fs533files[idx].PreferenceNodes = append(fs533files[idx].PreferenceNodes[:pendingIndex], fs533files[idx].PreferenceNodes[pendingIndex+1:]...)
	}

	updateFs533nodes()

	//specify destinate replication node
	for ridx, relocatedfile := range relocatedFiles {
		sort.Slice(fs533nodes, func(ii, jj int) bool {
			return fs533nodes[ii].TotalFileSize < fs533nodes[jj].TotalFileSize
		})

		for idx, fs533File := range fs533files {
			if fs533File.Fs533FileName != relocatedfile.fs533FileName {
				continue
			}

			for nidx := 0; nidx < len(fs533nodes); nidx++ {
				if relocatedFiles[ridx].srcReplicaNode != "" && relocatedFiles[ridx].dstReplicaNode != "" {
					break
				}
				if contains(fs533File.PreferenceNodes, fs533nodes[nidx].NodeAddress) {
					if relocatedFiles[ridx].srcReplicaNode == "" {
						relocatedFiles[ridx].srcReplicaNode = fs533nodes[nidx].NodeAddress
					}
				} else {
					if relocatedFiles[ridx].dstReplicaNode == "" {
						relocatedFiles[ridx].dstReplicaNode = fs533nodes[nidx].NodeAddress
						fs533files[idx].PreferenceNodes = append(fs533files[idx].PreferenceNodes, fs533nodes[nidx].NodeAddress)
					}
				}
			}
			updateFs533nodes()
			break
		}
	}

	c := make(chan bool)

	//start sending reallocate request to the target nodes
	for _, relocatedfile := range relocatedFiles {
		go sendBackupFileRequest(relocatedfile, c)
	}

	fileServer.Backup()

	for i := 0; i < len(relocatedFiles); i++ {
		<-c
	}

	log.Println(" ")
	log.Printf("******************* RE-REPLICATE REPORT **********************")
	log.Printf("Report for re-replicate process for node '%s'", nodeleaveAddress)
	log.Printf("Start time is on '%s'", ConvertTimeToLongString(startFiringTime))
	log.Printf("End time is on '%s'", ConvertTimeToLongString(time.Now()))
	log.Printf("Different is '%d' milliseconds", time.Now().Sub(startFiringTime).Milliseconds())

	log.Println("---------------------------------------------------------")
	var totalSize int32
	for _, rf := range relocatedFiles {
		log.Printf("Moved file '%s', version '%d', size '%d' from node '%s' to node '%s'", rf.fs533FileName, rf.fs533FileVersion, rf.fs533FileSize, rf.srcReplicaNode, rf.dstReplicaNode)
		totalSize = totalSize + rf.fs533FileSize
	}
	log.Println("Total file size is ", totalSize)

	log.Printf("******************* END RE-REPLICATE REPORT **********************")
	log.Println(" ")

}

//UpdateFs533ReplicaNodes ... update all the information of file operations that a master node should know
func (a *FileServer) UpdateFs533ReplicaNodes(newMember string) {
	anodemt := nodeMetadata{
		NodeAddress:   newMember,
		TotalFileSize: 0,
		Fs533files:    []string{},
	}
	fs533nodes = append(fs533nodes, anodemt)
}

//Backup ... backup all infomration about file operations that a master node should know
func (a *FileServer) Backup() {

	log.Printf("Send BACKUP request to all members.\n")

	var bpFs533Files []*fs533pb.BackupRequest_Fs533FileMetadata
	var bpPendingWriteTransactions []*fs533pb.BackupRequest_WriteTransaction

	for _, fs533File := range fs533files {
		bpFs533File := &fs533pb.BackupRequest_Fs533FileMetadata{
			Fs533FileName:    fs533File.Fs533FileName,
			Fs533FileSize:    fs533File.Fs533FileSize,
			Fs533FileVersion: fs533File.Fs533FileVersion,
			PreferenceNodes:  fs533File.PreferenceNodes,
		}
		bpFs533Files = append(bpFs533Files, bpFs533File)
	}

	for _, pendingWriteTransaction := range pendingWriteTransactions {
		bpPendingWriteTransaction := &fs533pb.BackupRequest_WriteTransaction{
			Fs533FileName:    pendingWriteTransaction.Fs533FileName,
			Fs533FileVersion: pendingWriteTransaction.Fs533FileVersion,
			RequestID:        pendingWriteTransaction.RequestID,
			PreferenceNodes:  pendingWriteTransaction.PreferenceNodes,
			RegisteredTime:   pendingWriteTransaction.RegisteredTime.UnixNano() / int64(time.Millisecond),
		}
		bpPendingWriteTransactions = append(bpPendingWriteTransactions, bpPendingWriteTransaction)
	}

	backupCounter = backupCounter + 1
	//gRPC call
	backupRequest := &fs533pb.BackupRequest{
		Fs533Files:               bpFs533Files,
		PendingWriteTransactions: bpPendingWriteTransactions,
		Counter:                  backupCounter,
	}

	for _, member := range membershipservice.GetFollowerAddresses() {
		requestSize := proto.Size(backupRequest)
		log.Println("************** BACKUP REQUEST to ", member, ": size = ", requestSize)
		cc, err := grpc.Dial(fmt.Sprintf("%s:%d", member, configuration.TCPPort), grpc.WithInsecure())
		if err != nil {
			errorMessage := fmt.Sprintf("could not connect to target node at %s due to error: %v\n", member, err.Error())
			log.Printf("Cannot send backup to node '%s'. Error: %s\n", member, errorMessage)
			break
		}

		defer cc.Close()

		//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
		duration := time.Duration(configuration.Timeout) * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()

		c := fs533pb.NewFileServiceClient(cc)

		backupResponse, err1 := c.Backup(ctx, backupRequest)
		responseSize := proto.Size(backupResponse)
		mslog(fmt.Sprintln("************** BACKUP RESPONSE to ", member, ": size = ", responseSize))
		if err1 != nil {
			log.Println("Error: It is unable to send BACKUP to node ", member, " due to error ", err1)
		}
	}

	log.Printf("Finish sending BACKUP to all members.\n")
}

//-------------------------------------------------GRPC APIs-------------------------------------------------------------------

//Locate ... register a write transaction
func (*grpcFileServer) Locate(ctx context.Context, req *fs533pb.LocateRequest) (*fs533pb.LocateResponse, error) {
	log.Printf("************** Receive LOCATE request for file '%s'.\n", req.GetFs533Filename())
	fs533filename := req.GetFs533Filename()
	fs533File := lookupFileMetadata(fs533filename)

	if fs533File.Fs533FileName == "" {
		err := fmt.Sprintf("Unable to locate file %s\n", fs533filename)
		log.Println(err)
		return nil, status.Errorf(codes.Internal, err)
	}

	var replicas []*fs533pb.LocateResponse_PreferenceNode

	for _, replica := range fs533File.PreferenceNodes {
		rep := &fs533pb.LocateResponse_PreferenceNode{
			NodebyAddress: replica,
		}
		replicas = append(replicas, rep)
	}

	resFile := &fs533pb.LocateResponse{
		Fs533Filename:    fs533File.Fs533FileName,
		Fs533FileSize:    fs533File.Fs533FileSize,
		Fs533FileVersion: fs533File.Fs533FileVersion,
		Replicas:         replicas,
	}

	log.Printf("************** Send LOCATE response for file '%s', it is replicated on '%s'.\n", fs533filename, replicas)

	return resFile, nil
}

//ListAll ... register a write transaction
func (*grpcFileServer) ListAll(ctx context.Context, req *fs533pb.Empty) (*fs533pb.AllFilesResponse, error) {
	log.Println("Receive LIST ALL request.")

	var files []*fs533pb.LocateResponse

	for _, fs533File := range fs533files {
		var replicas []*fs533pb.LocateResponse_PreferenceNode
		for _, replica := range fs533File.PreferenceNodes {
			rep := &fs533pb.LocateResponse_PreferenceNode{
				NodebyAddress: replica,
			}
			replicas = append(replicas, rep)
		}
		file := &fs533pb.LocateResponse{
			Fs533Filename:    fs533File.Fs533FileName,
			Fs533FileSize:    fs533File.Fs533FileSize,
			Fs533FileVersion: fs533File.Fs533FileVersion,
			Replicas:         replicas,
		}
		files = append(files, file)
	}

	allFilesResponse := &fs533pb.AllFilesResponse{
		Files: files,
	}

	log.Println("Send LIST ALL response")

	return allFilesResponse, nil
}

//DeleteAll this function is to delete all fs533
func (*grpcFileServer) DeleteAll(ctx context.Context, req *fs533pb.Empty) (*fs533pb.Empty, error) {
	fileHandler.ClearFs533Db()
	fileServer.UpdateMetada([]Fs533FileMetadata{}, []WriteTransaction{})

	return &fs533pb.Empty{}, nil
}

//Promote ... register a write transaction
func (*grpcFileServer) Vote(ctx context.Context, req *fs533pb.VotingRequest) (*fs533pb.VotingResponse, error) {
	eTerm := req.GetElectionTerm()
	candidateAddress := req.GetCandidateAddress()

	log.Printf("Receive VOTE request from node '%s' to self-promote as the new master of election term '%d'.\n", candidateAddress, eTerm)

	alreadyVoted := false
	for _, voteRecord := range voteRecords {
		if voteRecord == eTerm {
			alreadyVoted = true
		}
	}

	if alreadyVoted {
		log.Printf("Send VOTE response to node '%s', but it is not accepted.\n", candidateAddress)

		return &fs533pb.VotingResponse{Accepted: false}, nil
	}
	voteRecords = append(voteRecords, eTerm)
	log.Printf("Send VOTE response to node '%s', it is accepted.\n", candidateAddress)

	return &fs533pb.VotingResponse{Accepted: true}, nil
}

//Announce ... receive an announce from a node that it will become the new master
func (*grpcFileServer) Announce(ctx context.Context, req *fs533pb.AnnouncementRequest) (*fs533pb.Empty, error) {
	newMasterAddress := req.GetNewMasterAddress()

	log.Printf("Receive ANNOUNCE request from node '%s' to confirm that it would become the new master.\n", newMasterAddress)

	//reset the vote records
	voteRecords = []int32{}
	masterNode = newMasterAddress
	onElectionProcess = false

	return &fs533pb.Empty{}, nil
}

//CopyReplica ... receive a file replica from another node
func (*grpcFileServer) CopyReplica(stream fs533pb.FileService_CopyReplicaServer) error {
	//this function is proceeded on replica nodes. It will save the file on a temporary file buffer
	log.Printf("Receive COPY REPLICA request.\n")

	//receive file streaming and save it to a temporary file
	guid := uuid.New()
	tempfilename := fmt.Sprintf("___tempfile_%s", guid)
	tempfile := fileHandler.CreateOrOpenFile(fileHandler.Fs533FilePath(tempfilename))

	var fs533filename string
	var fs533fileversion int32
	var fs533filesize int32

	totalFileSize := 0
	counting := 0
	log.Printf("copy replica in progress ...")
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return status.Errorf(codes.Internal, fmt.Sprintf("failed unexpectadely while reading chunks from stream for file: %v due to internal error", fs533filename))
		}

		nb, err := tempfile.Write(req.GetFs533Filevalue())
		if err != nil {
			log.Printf("Error while reading chunks from stream to file: %v\n", err.Error())
		}
		totalFileSize = totalFileSize + nb
		if counting%10000 == 0 {
			fmt.Print(".")
		}
		counting++
		fs533filename = req.GetFs533Filename()
		fs533fileversion = req.GetFileVersion()
		fs533filesize = req.GetFs533FileSize()
	}

	log.Printf("Write %d byte from stream to file %s \n", totalFileSize, tempfilename)

	fileHandler.Copy(fileHandler.Fs533FilePath(tempfilename), fileHandler.Fs533FilePath(fs533filename))
	fileHandler.Delete(fileHandler.Fs533FilePath(tempfilename))

	//update local replica storage
	newReplica := Fs533FileMetadata{
		Fs533FileName:    fs533filename,
		Fs533FileVersion: fs533fileversion,
		Fs533FileSize:    fs533filesize,
	}
	replicaFiles = append(replicaFiles, newReplica)

	log.Printf("Finish COPY REPLICA request.\n")

	return nil
}

//BackupFile ... handle request to clone a replica to the new target node
func (*grpcFileServer) BackupFile(ctx context.Context, req *fs533pb.BackupFileRequest) (*fs533pb.Empty, error) {
	fs533FileName := req.GetFs533FileName()
	fs533FileVersion := req.GetFs533FileVersion()
	fs533FileSize := req.GetFs533FileSize()
	targetNodeAddress := req.GetTargetNode()
	log.Printf("Receive BACKUP FILE request to copy replica of file %s to the target node %s.\n", fs533FileName, targetNodeAddress)

	//open file for reading
	file := fileHandler.OpenFile(fileHandler.Fs533FilePath(fs533FileName))
	if file == nil {
		log.Printf("Could not open the replica of file %s\n", fs533FileName)
		return &fs533pb.Empty{}, nil
	}

	defer file.Close()

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", targetNodeAddress, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s due to error: %v\n", targetNodeAddress, err.Error())
		log.Printf("Cannot proceed write preparation request for file '%s'. Error: %s\n", fs533FileName, errorMessage)
		return &fs533pb.Empty{}, nil
	}

	defer cc.Close()

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout*100) * time.Second
	_, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewFileServiceClient(cc)

	stream, err := c.CopyReplica(context.Background())
	if err != nil {
		log.Printf("error while calling WritePrepare: %v", err)
		return &fs533pb.Empty{}, nil
	}

	//Read file in a buf of chunk size and then stream it back to client
	buf := make([]byte, configuration.FileChunkSize)

	totalRequestSize := 0
	writing := true
	for writing {
		// put as many bytes as `chunkSize` into the buf array.
		_, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		copyReplicaRequest := &fs533pb.CopyReplicaRequest{
			Fs533Filename:  fs533FileName,
			Fs533Filevalue: buf,
			FileVersion:    fs533FileVersion,
			Fs533FileSize:  fs533FileSize,
		}
		totalRequestSize = totalRequestSize + proto.Size(copyReplicaRequest)
		stream.Send(copyReplicaRequest)
	}

	log.Printf("************** COPY REPLICA REQUEST from '%s' to '%s', size = '%d'", configuration.IPAddress, targetNodeAddress, totalRequestSize)

	copyReplicaResponse, _ := stream.CloseAndRecv()
	// if err1 != nil {
	// 	log.Printf("error while streaming replicated file '%s' for BACKUP FILE request to node '%s': %v", fs533FileName, targetNodeAddress, err1)
	// 	return &fs533pb.Empty{}, nil
	// }
	responseSize := proto.Size(copyReplicaResponse)
	log.Printf("************** COPY REPLICA RESPONSE from '%s' to '%s', size = '%d'", configuration.IPAddress, targetNodeAddress, responseSize)

	log.Printf("Send BACKUP FILE response to copy replica of file %s to the target node %s.\n", fs533FileName, targetNodeAddress)
	return &fs533pb.Empty{}, nil
}

//Promote ... accept a promote request from master node
func (*grpcFileServer) Promote(ctx context.Context, req *fs533pb.PromoteRequest) (*fs533pb.Empty, error) {
	newMasterNode := req.GetNewMasterNode()
	log.Printf("Receive PROMOTE request for new master node which is %s.\n", newMasterNode)

	masterNode = newMasterNode
	electionTerm = electionTerm + 1

	return &fs533pb.Empty{}, nil
}

//Backup ... backup the local files metadata with the metadata sent by master node
func (*grpcFileServer) Backup(ctx context.Context, req *fs533pb.BackupRequest) (*fs533pb.BackupResponse, error) {
	log.Printf("Receive BACK UP request.\n")

	var initFs533Files []Fs533FileMetadata
	var initPendingWriteTransactions []WriteTransaction

	bpFs533Files := req.GetFs533Files()
	bpPendingWriteTransactions := req.GetPendingWriteTransactions()
	counter := req.GetCounter()

	//should not update file metadata if the backup request counter is less than file server's current counter
	if counter <= backupCounter {
		err := fmt.Sprintf("Warning: backup request '%d' will not be proceeded since it is already out of data than the current backup counter '%d'.\n", counter, backupCounter)
		log.Println(err)
		return &fs533pb.BackupResponse{}, status.Errorf(codes.Internal, err)
	}

	backupCounter = counter

	for _, pbFs533File := range bpFs533Files {
		initFs533File := Fs533FileMetadata{
			Fs533FileName:    pbFs533File.Fs533FileName,
			Fs533FileSize:    pbFs533File.Fs533FileSize,
			Fs533FileVersion: pbFs533File.Fs533FileVersion,
			PreferenceNodes:  pbFs533File.PreferenceNodes,
		}
		initFs533Files = append(initFs533Files, initFs533File)
	}

	for _, bpPendingWriteTransaction := range bpPendingWriteTransactions {
		initPendingWriteTransaction := WriteTransaction{
			Fs533FileName:    bpPendingWriteTransaction.Fs533FileName,
			Fs533FileVersion: bpPendingWriteTransaction.Fs533FileVersion,
			RequestID:        bpPendingWriteTransaction.RequestID,
			PreferenceNodes:  bpPendingWriteTransaction.PreferenceNodes,
			RegisteredTime:   time.Unix(0, bpPendingWriteTransaction.RegisteredTime*int64(time.Millisecond)),
		}
		initPendingWriteTransactions = append(initPendingWriteTransactions, initPendingWriteTransaction)
	}

	fileServer.UpdateMetada(initFs533Files, initPendingWriteTransactions)

	log.Printf("Send BACK UP response.\n")

	return &fs533pb.BackupResponse{}, nil
}

//WriteRegister ... register a write transaction
func (*grpcFileServer) WriteRegister(ctx context.Context, req *fs533pb.WriteRegisterRequest) (*fs533pb.WriteRegisterResponse, error) {
	fs533filename := req.GetFs533Filename()
	log.Printf("************** Receive WRITE REGISTRATION request for file '%s'.\n", fs533filename)
	startRegisteredTime := time.Now()
	var newFileVersion int32
	var preferenceNodes []string

	fs533fileMetadata := lookupFileMetadata(fs533filename)

	var latestTime time.Time

	if fs533fileMetadata.Fs533FileName == "" {
		log.Printf("The file %s is new so master node will allocate new replica nodes for it.\n", fs533filename)
		//Start allocating replica nodes process
		preferenceNodes = allowcatePreferenceNodes()
		newFileVersion = 1
	} else {
		//use existing replica nodes
		preferenceNodes = fs533fileMetadata.PreferenceNodes
		latestTime = fs533fileMetadata.RegisteredTime
		newFileVersion = fs533fileMetadata.Fs533FileVersion + 1
	}

	//push new write transaction into write transactions buffer
	writeTransaction := WriteTransaction{
		Fs533FileName:    fs533filename,
		Fs533FileVersion: newFileVersion,
		RequestID:        req.GetRequestId(),
		PreferenceNodes:  preferenceNodes,
		RegisteredTime:   time.Now(),
	}

	//this write request needs user interaction if it is not the first write for this file and it is initiated within 1 min from the previous one.
	needUserInteraction := false

	//query all the pending write transaction for the fs533 file
	var latestPendingWriteTransaction WriteTransaction
	for _, writeTransaction := range pendingWriteTransactions {
		if writeTransaction.Fs533FileName == fs533filename && writeTransaction.Fs533FileVersion > latestPendingWriteTransaction.Fs533FileVersion {
			latestPendingWriteTransaction = writeTransaction
		}
	}

	//if there is pending transaction for the same file, the new file version must be greater than that pending transaction
	if latestPendingWriteTransaction.Fs533FileVersion >= newFileVersion {
		newFileVersion = latestPendingWriteTransaction.Fs533FileVersion + 1
		latestTime = latestPendingWriteTransaction.RegisteredTime
	}

	if newFileVersion > 1 {
		timeDiff := writeTransaction.RegisteredTime.Sub(latestTime).Milliseconds()
		if timeDiff <= 60000 {
			log.Println(" ")
			log.Printf("******************* W-W CONFLICT REPORT **********************")
			log.Printf("Write conflict on file '%s'", fs533filename)
			log.Printf("Prior registered write is for version '%d' at '%s'", newFileVersion-1, ConvertTimeToLongString(latestTime))
			log.Printf("Current registered write is for version '%d' at '%s'", newFileVersion, ConvertTimeToLongString(writeTransaction.RegisteredTime))
			log.Printf("W-W confict process starts at '%s'", ConvertTimeToLongString(startRegisteredTime))
			log.Printf("W-W confict process starts at '%s'", ConvertTimeToLongString(time.Now()))
			log.Printf("W-W confict process takes '%d' microseconds", time.Now().Sub(startRegisteredTime).Microseconds())
			needUserInteraction = true
			log.Printf("******************* END W-W CONFLICT REPORT **********************")
			log.Println(" ")
		}
	}

	pendingWriteTransactions = append(pendingWriteTransactions, writeTransaction)
	fileServer.Backup()

	writeRegistrationResponse := &fs533pb.WriteRegisterResponse{
		PreferenceNodes:     preferenceNodes,
		FileVersion:         newFileVersion,
		NeedUserInteraction: needUserInteraction,
	}

	log.Printf("************** Send WRITE REGISTER response for file '%s': version %d, preference nodes are '%s', need user interaction is %v.\n", fs533filename, newFileVersion, preferenceNodes, needUserInteraction)

	return writeRegistrationResponse, nil
}

//WritePrepare ... uploading file to replica to prepare for a write transaction
func (*grpcFileServer) WritePrepare(stream fs533pb.FileService_WritePrepareServer) error {
	//this function is proceeded on replica nodes. It will save the file on a temporary file buffer
	log.Printf("************** Receive WRITE PREPARATION request.\n")

	//total file size
	var fs533filesize int32
	//receive file streaming and save it to a temporary file
	guid := uuid.New()
	tempfilename := fmt.Sprintf("___tempfile_%s", guid)
	tempfile := fileHandler.CreateOrOpenFile(fileHandler.Fs533FilePath(tempfilename))

	var fs533filename string
	var fs533fileversion int32
	var requestID string
	// while there are messages coming
	log.Printf("preparation in progress ...")
	totalSize := 0
	counting := 0
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return status.Errorf(codes.Internal, fmt.Sprintf("failed unexpectadely while reading chunks from stream for file: %v due to internal error %s", fs533filename, err))
		}

		nb, err := tempfile.Write(req.GetFs533Filevalue())
		if err != nil {
			log.Printf("Error while reading chunks from stream to file: %v\n", err.Error())
			return status.Errorf(codes.Internal, fmt.Sprintf("Error while writing to file %s due to err %v\n", fs533filename, err))
		}

		totalSize = totalSize + nb

		fs533filesize = fs533filesize + int32(nb)
		fs533filename = req.GetFs533Filename()
		fs533fileversion = req.GetFileVersion()
		requestID = req.GetRequestId()
		if counting%10000 == 0 {
			fmt.Print(".")
		}
		counting++
	}
	log.Printf("Write %d byte from client to file %s \n", totalSize, tempfilename)
	tempfile.Close()

	temporaryFile := TemporaryFs533File{
		Fs533FileName:     fs533filename,
		Fs533FileVersion:  fs533fileversion,
		RequestID:         requestID,
		TemporaryFileName: tempfilename,
		Fs533FileSize:     fs533filesize,
	}

	temporaryFiles = append(temporaryFiles, temporaryFile)

	log.Printf("************** Send WRITE PREPARATION response for file %s.\n", fs533filename)

	return stream.SendAndClose(&fs533pb.WritePreparationResponse{FileSize: fs533filesize})
}

//WriteEnd ... end write transaction by committing or aborting it
func (*grpcFileServer) WriteEnd(ctx context.Context, req *fs533pb.WriteEndRequest) (*fs533pb.WriteEndResponse, error) {
	//Update node capacity
	log.Printf("************** Receive WRITE END request for file '%s'.\n", req.GetFs533Filename())

	writeEndResponse := &fs533pb.WriteEndResponse{}

	fs533filename := req.GetFs533Filename()
	requestID := req.GetRequestId()
	fileSize := req.GetFileSize()
	commit := req.GetCommit()
	cancelWriteRegistration := req.GetCancelWriteRegistration()

	//look up all pending write transactions by fs533filename
	var pwrRequestID WriteTransaction
	var priorPwrOfTheFile WriteTransaction
	var preferenceNodes []string

	//if it is not master node, just simply commit the transaction by delete temp file and update local replica database
	if !isMasterNode() {
		commitLocalWriteTransaction(fs533filename, requestID, commit)

		//if it is not master node, that would be enough to send response to client
		log.Printf("************** Send WRITE END response for file '%s'.\n", fs533filename)
		return writeEndResponse, nil
	}

	var result bool

	//look up the pending write transaction for the requestId and its prior transaction for the same file
	for _, tran := range pendingWriteTransactions {
		if tran.Fs533FileName == fs533filename {
			if tran.RequestID == requestID {
				pwrRequestID = tran
			} else if tran.Fs533FileVersion > priorPwrOfTheFile.Fs533FileVersion && priorPwrOfTheFile.Fs533FileVersion < pwrRequestID.Fs533FileVersion {
				priorPwrOfTheFile = tran
			}
		}
	}

	//if there is no pending transaction of the requestID, there must be something wrong, abort the transaction
	if pwrRequestID.Fs533FileName == "" {
		//lookup preference nodes of this file
		fs533fileMetadata := lookupFileMetadata(fs533filename)

		if fs533fileMetadata.Fs533FileName == "" {
			log.Printf("The file '%s' is not found on any replica so error will be sent back to client.\n", fs533filename)
			return writeEndResponse, status.Errorf(codes.Internal, fmt.Sprintf("Could not find appropriate transaction for file: %v", fs533filename))
		}

		//sending abort to existing replica nodes of that file
		preferenceNodes = fs533fileMetadata.PreferenceNodes

		result = sendWriteCommitToReplicas(fs533filename, int32(0), requestID, false, preferenceNodes)
		if !result {
			return writeEndResponse, status.Errorf(codes.Internal, fmt.Sprintf("Could not find finish write transaction for file: %v due to internal error", fs533filename))
		}
		return writeEndResponse, status.Errorf(codes.Internal, fmt.Sprintf("Write transaction for file: %v is already aborted", fs533filename))
	}

	//If it is master node, sending write end request to the preference nodes to finish all the pendding write transactions
	//send commit to all the replica and wait until receiving a number of write quorum acks
	if !cancelWriteRegistration {
		preferenceNodes = pwrRequestID.PreferenceNodes
		priorTransactionIsCommitted := true
		if priorPwrOfTheFile.Fs533FileName != "" {
			//if there is a prior pending write transaction,
			// wait untill the prior write request is commit, or it reaches a timeout
			priorTransactionIsCommitted = checkIfPriorTransactionIsCommitted(priorPwrOfTheFile.RequestID)
		}

		//if prior transaction is not committed yet, abort the write transaction
		if !priorTransactionIsCommitted {
			log.Printf("Prior write transaction for this file has not completed and it reaches timeout so master will force aborting the prior transaction of request '%s'. \n", priorPwrOfTheFile.RequestID)
			//only master node is able to force aborting pending write transaction to the other nodes.
			forceAbortingWriteTransaction(priorPwrOfTheFile)
			//Otherwise, the current transaction should be aborted
			return writeEndResponse, status.Errorf(codes.Internal, fmt.Sprintf("Could not find finish write transaction for file: %v since the prior transaction is not committed yet", fs533filename))
		}

		sendWriteCommitToReplicas(fs533filename, fileSize, requestID, commit, preferenceNodes)
		updateFileMetadata(pwrRequestID, fileSize)
	}
	removePendingWriteTransaction(requestID)
	fileServer.Backup()

	log.Printf("************** Send WRITE END response for file '%s'.\n", fs533filename)
	return writeEndResponse, nil
}

//force aborting write transaction by removing it from pending write transaction and sending writeend request to the other nodes
func forceAbortingWriteTransaction(priorPwrOfTheFile WriteTransaction) {
	removePendingWriteTransaction(priorPwrOfTheFile.RequestID)
	sendWriteCommitToReplicas(priorPwrOfTheFile.Fs533FileName, 0, priorPwrOfTheFile.RequestID, false, priorPwrOfTheFile.PreferenceNodes)
}

//Read ... handle read request, it will stream the replicated file to client. This API will be processed on any replica nodes
func (*grpcFileServer) Read(req *fs533pb.ReadRequest, stream fs533pb.FileService_ReadServer) error {
	fs533filename := req.GetFs533Filename()
	log.Printf("Receive READ request for file %s.\n", fs533filename)

	//Looking up on its replica files
	var foundReplica Fs533FileMetadata
	for _, replica := range replicaFiles {
		if replica.Fs533FileName == fs533filename && replica.Fs533FileVersion == req.GetFileVersion() {
			foundReplica = replica
			break
		}
	}
	if foundReplica.Fs533FileName == "" {
		err := status.Errorf(codes.Internal, "Unable to read file %s since it is not found on the local storage", fs533filename)
		return err
	}

	file := fileHandler.OpenFile(fileHandler.Fs533FilePath(fs533filename))
	if file == nil {
		return status.Errorf(codes.Internal, fmt.Sprintf("Could not open file: %v", fs533filename))
	}

	defer file.Close()

	//Read file in a buf of chunk size and then stream it back to client
	buf := make([]byte, configuration.FileChunkSize)

	writing := true
	for writing {
		// put as many bytes as `chunkSize` into the buf array.
		n, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		stream.Send(
			&fs533pb.ReadResponse{
				Fs533Filevalue: buf[:n],
			})
	}
	return nil
}

//ReadAcquire ... acquire the highest version of file and the replica nodes which are storing its replicas
func (*grpcFileServer) ReadAcquire(ctx context.Context, req *fs533pb.ReadAcquireRequest) (*fs533pb.ReadAcquireResponse, error) {
	fs533filename := req.GetFs533Filename()
	log.Printf("Receive READ ACQUIRE request for file '%s'.\n", fs533filename)

	fs533fileMetadata := lookupFileMetadata(fs533filename)

	readAcquireResponse := &fs533pb.ReadAcquireResponse{
		Status: false,
	}

	if fs533fileMetadata.Fs533FileName == "" {
		log.Printf("Could not find the file %s on any replica.\n", fs533filename)
		err := status.Errorf(codes.Internal, "unable to acquire read for file %s since it is not found on any replica", fs533filename)
		return readAcquireResponse, err
	}

	//if this is NOT master node, it will just simply return file metadata on its local storage
	if !isMasterNode() {
		for _, replicatedFile := range replicaFiles {
			if replicatedFile.Fs533FileName == fs533filename {
				readAcquireResponse.Status = true
				readAcquireResponse.FileVersion = replicatedFile.Fs533FileVersion
				break
			}
		}
		if readAcquireResponse.Status {
			log.Printf("Send READ ACQUIRE response for file '%s'. The replicated file was found with version '%d'\n", fs533filename, readAcquireResponse.GetFileVersion())
		} else {
			log.Printf("Send READ ACQUIRE response for file '%s'. There is replicated file was found", fs533filename)
		}

		return readAcquireResponse, nil
	}

	var highestFileVersion int32
	var preferencesNodes []string

	highestFileVersion = -1
	numberOfAcksReceived := 0
	numberOfWaitingAcks := 0
	c := make(chan readAcquireResult)
	for _, preferenceNode := range fs533fileMetadata.PreferenceNodes {
		//if preferenceNode is the masternode itself then just simply return the file version on its replica list
		if preferenceNode == masterNode {
			foundReplica := false
			for _, replica := range replicaFiles {
				if replica.Fs533FileName == fs533fileMetadata.Fs533FileName {
					if replica.Fs533FileVersion > highestFileVersion {
						highestFileVersion = replica.Fs533FileVersion
						preferencesNodes = append(preferencesNodes, masterNode)
					}
					foundReplica = true
					break
				}
			}
			if foundReplica {
				numberOfAcksReceived++
			} else {
				log.Printf("Warning: it is unable to find a replica of %s on the node %s", fs533filename, preferenceNode)
			}
			continue
		}
		numberOfWaitingAcks++
		go sendReadAcquireToReplica(fs533filename, preferenceNode, c)
	}

	for i := 0; i < numberOfWaitingAcks; i++ {
		raResult := <-c
		if raResult.status {

			numberOfAcksReceived++
			if raResult.version >= highestFileVersion {
				highestFileVersion = raResult.version
				preferencesNodes = append(preferencesNodes, raResult.targetNode)
			}
		}
		if reachWriteQuorum(numberOfAcksReceived) {
			readAcquireResponse.FileVersion = highestFileVersion
			readAcquireResponse.PreferenceNodes = preferencesNodes
			readAcquireResponse.Status = true
			log.Printf("Send READ ACQUIRE response for file '%s'. The replicated file was found with version '%d' on  nodes '%s'\n", fs533filename, highestFileVersion, preferencesNodes)

			return readAcquireResponse, nil
		}
	}

	err := status.Errorf(codes.Internal, "unable to acquire read for file %s since it is unable to receive enough ack from the replicas", fs533filename)

	return readAcquireResponse, err
}

//Delete ... removing file handler which will be handled on master node
func (*grpcFileServer) Delete(ctx context.Context, req *fs533pb.DeleteRequest) (*fs533pb.DeleteResponse, error) {
	fs533filename := req.GetFs533Filename()
	log.Printf("Receive DELETE file %s request.\n", fs533filename)

	deleteResponse := &fs533pb.DeleteResponse{
		Status: true,
	}

	//if this is not master node, it will just simply delete the file and return the ack
	if !isMasterNode() {
		fileHandler.Delete(fileHandler.Fs533FilePath(fs533filename))
		//update local replica list
		removeFileOnLocalReplicatedFiles(fs533filename)
		return deleteResponse, nil
	}

	fs533fileMetadata := lookupFileMetadata(fs533filename)

	numberOfAcksReceived := 0
	numberOfWaitingAcks := 0
	c := make(chan bool)
	preferenceNodes := fs533fileMetadata.PreferenceNodes
	for _, preferenceNode := range preferenceNodes {
		//if preferenceNode is the masternode itself then just simply delete the file and incerase the number of ack
		if preferenceNode == masterNode {
			fileHandler.Delete(fileHandler.Fs533FilePath(fs533filename))
			numberOfAcksReceived++
			continue
		}
		numberOfWaitingAcks++
		go sendDeleteRequestToReplica(fs533filename, preferenceNode, c)
	}

	for i := 0; i < numberOfWaitingAcks; i++ {
		ackReceived := <-c
		if ackReceived {
			numberOfAcksReceived++
		}
		if numberOfAcksReceived >= configuration.WriteQuorum {
			//update fs533files
			removeFileOnFs533Files(fs533filename)

			fileServer.Backup()
			return deleteResponse, nil
		}
	}

	deleteResponse.Status = false
	err := status.Errorf(codes.Internal, "unable to delete file name since it is unable to receive enough ack from the replicas")
	return deleteResponse, err
}

//------------------------------------------------PRIVATE FUNCTIONS----------------------------------------------------------------

//send voting requests to all the other nodes to self-promote as a new master
func sendVotingRequests() bool {
	//if it voted for another node already then it should not vote for itself on the same election term

	alreadyVoted := false
	for _, voteRecord := range voteRecords {
		if voteRecord == electionTerm {
			alreadyVoted = true
		}
	}
	if alreadyVoted && onElectionProcess && membershipservice.Len() == 2 {
		return false
	}

	numberOfWaitingAcks := 0
	c := make(chan bool)
	for _, member := range membershipservice.GetFollowerAddresses() {
		if member == configuration.IPAddress {
			continue
		}
		numberOfWaitingAcks++
		go sendVotingRequest(member, c)
	}

	numberOfAcceptance := 0
	for i := 0; i < numberOfWaitingAcks; i++ {
		r := <-c
		if r {
			numberOfAcceptance++
		}
		if numberOfAcceptance > countTheMajorityNumber(numberOfWaitingAcks) || (numberOfAcceptance == 1 && membershipservice.Len() == 2) {
			return true
		}
	}
	return false
}

//send vote request to the tartget node
func sendVotingRequest(targetNode string, cResult chan bool) {
	log.Printf("Send VOTE request to node %s to self-promote %s as new master.\n", targetNode, configuration.IPAddress)
	votingRequest := &fs533pb.VotingRequest{
		ElectionTerm:     electionTerm,
		CandidateAddress: configuration.IPAddress,
	}

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", targetNode, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s due to error: %v\n", targetNode, err.Error())
		log.Printf("Cannot send promote request to node '%s'. Error: %s\n", targetNode, errorMessage)
		cResult <- false
		return
	}

	defer cc.Close()

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewFileServiceClient(cc)

	_, err1 := c.Vote(ctx, votingRequest)

	if err1 != nil {
		log.Println("Error: It is unable to send promote request to node ", targetNode, " due to error ", err)
		cResult <- false
		return
	}
	cResult <- true
}

//return the number of members which should be consider as a majority number
func countTheMajorityNumber(memberCount int) int {
	return int(math.Ceil(float64(memberCount) / float64(2)))
}

//send announcement about the new master role to the other nodes
func sendAnnouncementRequests() {
	log.Printf("Send ANNOUNCEMENT request to all members about new master role.\n")
	announcementRequest := &fs533pb.AnnouncementRequest{
		NewMasterAddress: configuration.IPAddress,
	}

	for _, member := range membershipservice.GetFollowerAddresses() {
		if member == configuration.IPAddress {
			continue
		}

		cc, err := grpc.Dial(fmt.Sprintf("%s:%d", member, configuration.TCPPort), grpc.WithInsecure())
		if err != nil {
			errorMessage := fmt.Sprintf("could not connect to target node at %s due to error: %v\n", member, err.Error())
			log.Printf("Cannot send promote request to node '%s'. Error: %s\n", member, errorMessage)
			continue
		}

		defer cc.Close()

		//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
		duration := time.Duration(configuration.Timeout) * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()

		c := fs533pb.NewFileServiceClient(cc)

		_, err1 := c.Announce(ctx, announcementRequest)

		if err1 != nil {
			log.Println("Error: It is unable to send promote request to node ", member, " due to error ", err)
		}
	}
}

//send request to a node storing replica of the file to clone it to the target node
func sendBackupFileRequest(relocatedFile relocatedFileMetadata, ackChan chan bool) {
	log.Printf("Send BACKUP FILE request to replicate file '%s' from node '%s' to node '%s'.\n", relocatedFile.fs533FileName, relocatedFile.srcReplicaNode, relocatedFile.dstReplicaNode)

	//gRPC call
	backupFileRequest := &fs533pb.BackupFileRequest{
		TargetNode:       relocatedFile.dstReplicaNode,
		Fs533FileName:    relocatedFile.fs533FileName,
		Fs533FileVersion: relocatedFile.fs533FileVersion,
		Fs533FileSize:    relocatedFile.fs533FileSize,
	}
	requestSize := proto.Size(backupFileRequest)
	log.Println("************** BACKUP FILE REQUEST to '", relocatedFile.srcReplicaNode, "': size = ", requestSize)

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", relocatedFile.srcReplicaNode, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s due to error: %v\n", relocatedFile.srcReplicaNode, err.Error())
		log.Printf("Cannot send backup to node '%s'. Error: %s\n", relocatedFile.srcReplicaNode, errorMessage)
		return
	}

	defer cc.Close()

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewFileServiceClient(cc)

	backupFileResponse, err1 := c.BackupFile(ctx, backupFileRequest)

	responseSize := proto.Size(backupFileResponse)
	log.Println("************** BACKUP FILE RESPONSE to '", relocatedFile.srcReplicaNode, "': size = ", responseSize)

	if err1 != nil {
		log.Printf("Error: It is unable to BACKUP FILE '%s' from node '%s' to node '%s' due to error '%s'.\n", relocatedFile.fs533FileName, relocatedFile.srcReplicaNode, relocatedFile.srcReplicaNode, err1)
		ackChan <- false
		return
	}

	ackChan <- true
	log.Printf("End BACKUP FILE request for file '%s' from node '%s' to node %s.\n", relocatedFile.fs533FileName, relocatedFile.srcReplicaNode, relocatedFile.dstReplicaNode)
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

//send request to all the node to promote the new master node
func sendPromoteRequest(newMasterNode string, targetNode string) {
	log.Printf("Send PROMOTE request to node %s to promote %s as new master.\n", targetNode, newMasterNode)
	promoteRequest := &fs533pb.PromoteRequest{
		NewMasterNode: newMasterNode,
	}

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", targetNode, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s due to error: %v\n", targetNode, err.Error())
		log.Printf("Cannot send promote request to node '%s'. Error: %s\n", targetNode, errorMessage)
		return
	}

	defer cc.Close()

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewFileServiceClient(cc)

	_, err1 := c.Promote(ctx, promoteRequest)

	if err1 != nil {
		log.Println("Error: It is unable to send promote request to node ", targetNode, " due to error ", err)
	}

	log.Printf("Finish sending PROMOTE request to node %s to promote %s as new master.\n", targetNode, newMasterNode)
}

//this function is to wait and check if the prio write transaction is committed or not.
//return true if it is committed. Otherwise, return false if It reaches timeout
func checkIfPriorTransactionIsCommitted(requestID string) bool {
	timeout := time.After(time.Duration(configuration.PendingWriteTransactionTimeoutInSeconds) * time.Second)
	tick := time.Tick(500 * time.Millisecond)
	// Keep trying until it is timed out or the transaction is updated
	for {
		select {
		case <-timeout:
			return false

		case <-tick:
			committed := true
			for _, tran := range pendingWriteTransactions {
				if tran.RequestID == requestID {
					committed = false
				}
			}
			if committed {
				return true
			}
		}
	}
}

//clean up the temporary file created on write transaction, it should be deleted after rename to the fs533filename
//if commit is true, the transaction is successful so the local replica files should be updated too
func commitLocalWriteTransaction(fs533filename string, requestID string, commit bool) {
	tempFileIndex := -1
	var fileSize int32
	var fileVersion int32
	var tempFile string

	for i := 0; i < len(temporaryFiles); i++ {
		if temporaryFiles[i].RequestID == requestID {
			tempFileIndex = i
			fileSize = temporaryFiles[i].Fs533FileSize
			fileVersion = temporaryFiles[i].Fs533FileVersion
			tempFile = temporaryFiles[i].TemporaryFileName
			break
		}
	}

	if commit {
		if fileVersion > 1 {
			for i := 0; i < len(replicaFiles); i++ {
				if replicaFiles[i].Fs533FileName == fs533filename {
					if fileVersion > replicaFiles[i].Fs533FileVersion {
						//only update replica with higher version than current one
						replicaFiles[i].Fs533FileSize = fileSize
						replicaFiles[i].Fs533FileVersion = fileVersion
					}
					break
				}
			}
		} else {
			newReplica := Fs533FileMetadata{
				Fs533FileName:    fs533filename,
				Fs533FileSize:    fileSize,
				Fs533FileVersion: fileVersion,
			}
			replicaFiles = append(replicaFiles, newReplica)
		}
	}

	if tempFileIndex != -1 {
		temporaryFiles = append(temporaryFiles[:tempFileIndex], temporaryFiles[tempFileIndex+1:]...)
		log.Printf("delete temporary file '%s' for file '%s'", tempFile, fs533filename)
		fileHandler.Copy(fileHandler.Fs533FilePath(tempFile), fileHandler.Fs533FilePath(fs533filename))
		fileHandler.Delete(fileHandler.Fs533FilePath(tempFile))
	} else {
		log.Printf("warning: could not find temporary file for file '%s'", fs533filename)
	}
}

//remove pending write transaction indicated by requestID
func removePendingWriteTransaction(requestID string) {
	pendingIndex := -1

	for i := 0; i < len(pendingWriteTransactions); i++ {
		if pendingWriteTransactions[i].RequestID == requestID {
			pendingIndex = i
			break
		}
	}
	pendingWriteTransactions = append(pendingWriteTransactions[:pendingIndex], pendingWriteTransactions[pendingIndex+1:]...)
}

//send write transaction commit request to replica nodes.
func sendWriteCommitToReplicas(fs533filename string, fs533filesize int32, requestID string, commit bool, preferenceNodes []string) bool {
	log.Printf("send WRITE END request to all perference nodes '%s'.\n", preferenceNodes)

	numberOfAcksReceived := 0
	numberOfWaitingAcks := 0
	c := make(chan bool)
	for _, preferenceNode := range preferenceNodes {
		//if preferenceNode is the masternode itself then just simply return the file version on its replica list
		if preferenceNode == masterNode {
			commitLocalWriteTransaction(fs533filename, requestID, commit)
			numberOfAcksReceived++
			continue
		}
		log.Printf("send WRITE END request to node '%s'.\n", preferenceNode)

		numberOfWaitingAcks++
		go sendWriteCommitToReplica(requestID, fs533filename, fs533filesize, preferenceNode, commit, c)
	}

	for i := 0; i < numberOfWaitingAcks; i++ {
		raResult := <-c
		if raResult {
			numberOfAcksReceived++
		}

		if reachWriteQuorum(numberOfAcksReceived) {
			return true
		}
	}
	return false
}

//send end write commit request to the replica node
func sendWriteCommitToReplica(requestID string, fs533filename string, fs533filesize int32, preferenceNode string, commit bool, ackChan chan bool) {
	ackChan <- fileClient.InternalEndWriteFile(requestID, fs533filename, fs533filesize, preferenceNode, false, commit)
}

//regenerate Fs533nodes
func updateFs533nodes() {
	var updatedFs533Nodes []nodeMetadata

	for _, fileMetadata := range fs533files {
		for _, node := range fileMetadata.PreferenceNodes {
			isAdded := false
			for idx, node2 := range updatedFs533Nodes {
				if node2.NodeAddress == node {
					isAdded = true
					updatedFs533Nodes[idx].TotalFileSize = updatedFs533Nodes[idx].TotalFileSize + fileMetadata.Fs533FileSize
					updatedFs533Nodes[idx].Fs533files = append(updatedFs533Nodes[idx].Fs533files, fileMetadata.Fs533FileName)
				}
			}
			if !isAdded {
				anodemt := nodeMetadata{
					NodeAddress:   node,
					TotalFileSize: fileMetadata.Fs533FileSize,
					Fs533files:    []string{fileMetadata.Fs533FileName},
				}
				updatedFs533Nodes = append(updatedFs533Nodes, anodemt)
			}
		}
	}

	for _, member := range membershipservice.GetNodeAddresses() {
		alreadyAdded := false
		for _, fs533node := range updatedFs533Nodes {
			if member == fs533node.NodeAddress {
				alreadyAdded = true
				break
			}
		}
		if !alreadyAdded {
			anodemt := nodeMetadata{
				NodeAddress:   member,
				TotalFileSize: 0,
				Fs533files:    []string{},
			}
			updatedFs533Nodes = append(updatedFs533Nodes, anodemt)
		}
	}

	fs533nodes = updatedFs533Nodes
}

//remove file on local replicated fs533 files which is managed by replica nonde
func removeFileOnLocalReplicatedFiles(fs533filename string) {
	fileIndex := -1

	for i := 0; i < len(replicaFiles); i++ {
		if replicaFiles[i].Fs533FileName == fs533filename {
			fileIndex = i
			break
		}
	}

	replicaFiles = append(replicaFiles[:fileIndex], replicaFiles[fileIndex+1:]...)
}

//remove file on global fs533 files which is managed by master nonde
func removeFileOnFs533Files(fs533filename string) {
	fileIndex := -1

	for i := 0; i < len(fs533files); i++ {
		if fs533files[i].Fs533FileName == fs533filename {
			fileIndex = i
			break
		}
	}

	fs533files = append(fs533files[:fileIndex], fs533files[fileIndex+1:]...)
	updateFs533nodes()
}

//this function is to allowcate new replica to the first N nodes having most capacity at the moment
func allowcatePreferenceNodes() []string {
	var result []string
	if len(fs533nodes) <= configuration.NumberOfReplicas {
		for _, node := range fs533nodes {
			result = append(result, node.NodeAddress)
		}
		return result
	}

	sort.Slice(fs533nodes, func(i, j int) bool {
		return fs533nodes[i].TotalFileSize < fs533nodes[j].TotalFileSize
	})

	for i := 0; i < configuration.NumberOfReplicas; i++ {
		result = append(result, fs533nodes[i].NodeAddress)
	}

	return result
}

//send read acquire request to the replica node that is storing the file being read
//return status, file version and preference nodes
func sendReadAcquireToReplica(fs533filename string, replicaNodeAddress string, ackChan chan readAcquireResult) {
	r1, r2, _ := fileClient.InternalReadAcquireFile(fs533filename, replicaNodeAddress)
	result := readAcquireResult{
		status:     r1,
		version:    int32(r2),
		targetNode: replicaNodeAddress,
	}
	ackChan <- result
}

//send delete request to the replica node that is storing the file being removed
func sendDeleteRequestToReplica(fs533filename string, replicaNodeAddress string, ackChan chan bool) {
	ackChan <- fileClient.InternalRemoveFile(fs533filename, replicaNodeAddress)
}

//verify if request reaches write quorum or not. It depends on the number of active nodes.
//If there is less than 3 nodes, the consensus reaches only if the number of acks is greater equal to the number of active nodes
//Otherwise, the consensus reaches if the number of acks is greater or equal the number of write quorum
func reachWriteQuorum(numberOfAcksReceived int) bool {
	if membershipservice.Len() < 3 {
		return numberOfAcksReceived >= membershipservice.Len()
	}
	return numberOfAcksReceived >= configuration.WriteQuorum
}

//lookup file metadata by its name on master node
func updateFileMetadata(writeTransaction WriteTransaction, fileSize int32) {
	updated := false
	for i := 0; i < len(fs533files); i++ {
		if fs533files[i].Fs533FileName == writeTransaction.Fs533FileName {
			updated = true
			fs533files[i].Fs533FileVersion = writeTransaction.Fs533FileVersion
			fs533files[i].Fs533FileSize = fileSize
			fs533files[i].RegisteredTime = writeTransaction.RegisteredTime

			break
		}
	}

	if !updated {
		newFileMetadata := Fs533FileMetadata{
			Fs533FileName:    writeTransaction.Fs533FileName,
			Fs533FileSize:    fileSize,
			Fs533FileVersion: writeTransaction.Fs533FileVersion,
			PreferenceNodes:  writeTransaction.PreferenceNodes,
			RegisteredTime:   writeTransaction.RegisteredTime,
		}
		fs533files = append(fs533files, newFileMetadata)
	}

	updateFs533nodes()
}

//lookup file metadata by its name on master node
func lookupFileMetadata(fs533filename string) Fs533FileMetadata {
	for _, fs533file := range fs533files {
		if fs533filename == fs533file.Fs533FileName {
			return fs533file
		}
	}
	return Fs533FileMetadata{
		Fs533FileName:    "",
		Fs533FileVersion: 0,
		Fs533FileSize:    0,
		PreferenceNodes:  []string{},
	}
}

//check if the node handling request is master node or not. There will be different processes for master node and follower nodes
func isMasterNode() bool {
	if masterNode == configuration.IPAddress {
		return true
	}
	return false
}
