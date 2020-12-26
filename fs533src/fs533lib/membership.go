package fs533lib

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	fs533pb "../fs533pb"
)

//MembershipService ... is a struct to encapsulate all properties and functions related to membership
type safeMembersCounter struct {
	activeMembersByAddress []string
	activeMembersByName    []string

	mux sync.Mutex
}

//MembershipService ... is a struct to expose public functions
type MembershipService struct {
	activeMembers    safeMembersCounter
	membershipStatus bool //0 is inactive | 1 is active

	needRestartSendingHeartBeats    bool
	needRestartMonitoringHeartBeats bool
}

type msServer struct{}

var configuration Configuration
var grpcServer *grpc.Server
var fileServer FileServer

var membershipservice MembershipService

//GetNodeAddresses ... This function is to return active members on the group, except the master node
func (a *MembershipService) GetNodeAddresses() []string {
	var result []string
	for _, member := range membershipservice.activeMembers.activeMembersByAddress {
		result = append(result, member)
	}
	return result
}

//GetFollowerAddresses ... This function is to return active members on the group, except the master node
func (a *MembershipService) GetFollowerAddresses() []string {
	var result []string
	for _, member := range membershipservice.activeMembers.activeMembersByAddress {
		if member == masterNode {
			continue
		}
		result = append(result, member)
	}
	return result
}

//Len ... This function is to return total active members on the group
func (a *MembershipService) Len() int {
	return membershipservice.activeMembers.len()
}

//SetConfiguration ... This function is to initialize the configuration
func (a *MembershipService) SetConfiguration(config Configuration) {
	configuration = config
}

//Initialize ... This function is to initialize the active members list
//arguments
//-byAddress: list of node ip addresses of all members
//-byName: list of node name including ip address and a timestamp of all members
func (a *MembershipService) Initialize(byAddress []string, byName []string) {
	if byAddress == nil {
		membershipservice.activeMembers.init([]string{configuration.IPAddress}, []string{generateNodeName(configuration.IPAddress)})
		mslog(fmt.Sprintf("Start membership list initialized with 1st member: %s.\n", printMembershiplist()))
	} else {
		membershipservice.activeMembers.init(byAddress, byName)
	}

	membershipservice.membershipStatus = true
	membershipservice.needRestartMonitoringHeartBeats = false
	membershipservice.needRestartSendingHeartBeats = false

	//set the masternode as itself
	masterNode = configuration.IPAddress
}

//Start ... start membership service
//Arguments
//- init: boolean. It will start a new membership group if init is true. Otherwise, it will send joining request to the gateway nodes
func (a *MembershipService) Start(init bool) {
	if init {
		membershipservice.Initialize(nil, nil)
		fileServer.Start(configuration.IPAddress, 0, []Fs533FileMetadata{}, []WriteTransaction{})
		fileHandler.ClearFs533Db()
	} else {
		for _, node := range configuration.GatewayNodes {
			mslog(fmt.Sprintln("START SENDING JOIN REQUEST to gateway node ", node))
			isSuccessful, membersList, master, eTerm, initFs533Files, initPendingWriteTransactions := sendJoiningRequest(configuration.IPAddress, "", node, true)
			if isSuccessful {
				activeMembersByAddress := []string{}
				activeMembersByName := membersList
				for _, nemberByName := range activeMembersByName {
					memArr := strings.Split(nemberByName, "-")
					activeMembersByAddress = append(activeMembersByAddress, memArr[0])
				}

				membershipservice.Initialize(activeMembersByAddress, activeMembersByName)
				fileServer.Start(master, eTerm, initFs533Files, initPendingWriteTransactions)
				fileHandler.ClearFs533Db()

				mslog(fmt.Sprintln("Finish joining request. Members list now is ", printMembershiplist()))
				break
			}
		}
	}
	startServices()
}

//Stop ... stop membership service
func (a *MembershipService) Stop() {
	//it leaves the group so set its status to 0
	membershipservice.membershipStatus = false
	membershipservice.needRestartSendingHeartBeats = true
	membershipservice.needRestartMonitoringHeartBeats = true
	//hblog(fmt.Sprintf("Testing ... 1. membershipservice.needRestartSendingHeartBeats = %v. membershipservice.membershipStatus =%v", membershipservice.needRestartSendingHeartBeats, membershipservice.membershipStatus))

	if isMasterNode() {
		mslog(fmt.Sprintf("Start leaving process for MASTER NODE '%s'.\n", configuration.IPAddress))
	} else {
		mslog(fmt.Sprintf("Start leaving process for node '%s'.\n", configuration.IPAddress))
	}

	memberIndex := getMemberIndex(configuration.IPAddress)
	membershipservice.activeMembers.removeMemberAtIndex(memberIndex)

	if isMasterNode() {
		fileServer.FireNodeLeave(configuration.IPAddress)
	}

	c := make(chan bool)
	count := 0
	for _, member := range membershipservice.activeMembers.getIds() {
		count++
		go sendLeavingRequest(configuration.IPAddress, member, c)
	}

	for i := 0; i < count; i++ {
		<-c
	}

	//empty the active list since it is no longer a member of the group
	membershipservice.Initialize([]string{}, []string{})
	stopMembershipService()
	fileServer.Stop()

	mslog(fmt.Sprintln("Finish leaving process"))
}

//Report ... report current active nodes on the membership
func (a *MembershipService) Report() {
	log.Println("----------------------------------MEMBERSHIP REPORT-------------------------------------")
	mslog(fmt.Sprintln("Members:  ", printMembershiplist()))
}

//-----------------------------------------membership service API----------------------------------------------
//Membership service APIs includes Join/Leave APIs
func (*msServer) Join(ctx context.Context, req *fs533pb.JoinRequest) (*fs533pb.JoinResponse, error) {
	//if the node is not on active members list yet, add it to the list and spread it to the other nodes if needed
	nodeAddress := req.GetNodeAddress()
	nodeName := req.GetNodeName()
	if nodeName == "" {
		nodeName = generateNodeName(nodeAddress)
	}
	mslog(fmt.Sprintf("RECEIVE NEW JOIN REQUEST from '%s' with option spreading new join is %t.\n", nodeAddress, req.GetNeedSpreading()))

	if getMemberIndex(req.GetNodeAddress()) == -1 {
		if req.GetNeedSpreading() {
			for _, targetNodeAddress := range membershipservice.activeMembers.getIds() {
				if targetNodeAddress != configuration.IPAddress {
					mslog(fmt.Sprintf("Start updating about new join to node %s:%d.\n", targetNodeAddress, configuration.TCPPort))
					go sendJoiningRequest(nodeAddress, nodeName, targetNodeAddress, false)
				}
			}
		}
		membershipservice.activeMembers.addMember(req.GetNodeAddress(), nodeName)
		fileServer.UpdateFs533ReplicaNodes(nodeAddress)
	} else {
		err := fmt.Sprintf("Node '%s' already joined the membership.\n", nodeAddress)
		mslog(fmt.Sprintln(err))
		return nil, status.Errorf(codes.Internal, err)
	}

	response := &fs533pb.JoinResponse{}

	//only response active list to the new comming node
	if !req.NeedSpreading {
		return response, nil
	}

	response.Members = membershipservice.activeMembers.getNames()

	var bpFs533Files []*fs533pb.JoinResponse_Fs533FileMetadata
	var bpPendingWriteTransactions []*fs533pb.JoinResponse_WriteTransaction

	for _, fs533File := range fs533files {
		bpFs533File := &fs533pb.JoinResponse_Fs533FileMetadata{
			Fs533FileName:    fs533File.Fs533FileName,
			Fs533FileSize:    fs533File.Fs533FileSize,
			Fs533FileVersion: fs533File.Fs533FileVersion,
			PreferenceNodes:  fs533File.PreferenceNodes,
		}
		bpFs533Files = append(bpFs533Files, bpFs533File)
	}

	for _, pendingWriteTransaction := range pendingWriteTransactions {
		bpPendingWriteTransaction := &fs533pb.JoinResponse_WriteTransaction{
			Fs533FileName:    pendingWriteTransaction.Fs533FileName,
			Fs533FileVersion: pendingWriteTransaction.Fs533FileVersion,
			RequestID:        pendingWriteTransaction.RequestID,
			PreferenceNodes:  pendingWriteTransaction.PreferenceNodes,
			RegisteredTime:   pendingWriteTransaction.RegisteredTime.UnixNano() / int64(time.Millisecond),
		}
		bpPendingWriteTransactions = append(bpPendingWriteTransactions, bpPendingWriteTransaction)
	}

	response.Fs533Files = bpFs533Files
	response.PendingWriteTransactions = bpPendingWriteTransactions
	response.MasterNode = masterNode
	response.ElectionTerm = electionTerm

	return response, nil
}

//leave API handler of the membership service
//arguments
//-context: the grpc connection context
//-leaverequest: the grpc request object
//returns
//-leaveresponse: the grpc reponse object
//-error: error message if there is
func (*msServer) Leave(ctx context.Context, req *fs533pb.LeaveRequest) (*fs533pb.LeaveResponse, error) {
	mslog(fmt.Sprintln("RECEIVE LEAVE REQUEST for node ", req.GetLeavingNodeAddress(), " from node ", req.GetRequesterNodeAddress()))

	memberIndex := getMemberIndex(req.GetLeavingNodeAddress())

	if memberIndex == -1 {
		err := fmt.Sprintf("Warning: This node '%s' is already removed from active membership list", req.GetLeavingNodeAddress())
		mslog(fmt.Sprintln(err))
		response := &fs533pb.LeaveResponse{
			LeaveStatus:  false,
			ErrorMessage: err,
		}

		return response, status.Errorf(codes.Internal, err)
	}

	requestMemberIndex := getMemberIndex(req.GetRequesterNodeAddress())
	if requestMemberIndex == -1 {
		err := fmt.Sprintf("Warning: This node '%s' is not allowed to send leaving request since it is not active on active membership list", req.GetLeavingNodeAddress())
		mslog(fmt.Sprintln(err))
		response := &fs533pb.LeaveResponse{
			LeaveStatus:  false,
			ErrorMessage: err,
		}

		return response, status.Errorf(codes.Internal, err)
	}

	//update activeMembersByAddress slice by removing the req.IpAddress out of the list
	membershipservice.activeMembers.removeMemberAtIndex(memberIndex)

	if isMasterNode() {
		fileServer.FireNodeLeave(req.GetLeavingNodeAddress())
	}

	//remove it from the predecessor
	for idx, predecessor := range predecessors.getValues() {
		if predecessor.NodeAddress == req.GetLeavingNodeAddress() {
			predecessors.set(idx, MonitorPacket{})
		}
	}

	response := &fs533pb.LeaveResponse{
		LeaveStatus: true,
	}

	return response, nil
}

//------------------------------------------private functions---------------------------------------------------
//stop grpc server
func stopMembershipService() {
	mslog(fmt.Sprintf("Stop membership service since membership status is inactive now"))

	grpcServer.Stop()
}

//send leave request to the gateway nodes
//arguments
//-leavingNodeAddress: the ip address of the node which is leaving
//-targetNodeAddress: the ip address of the active member which will handle leave request
//-returnChan: return to the main rountine
func sendLeavingRequest(leavingNodeAddress string, targetNodeAddress string, returnChan chan bool) {
	//Sending rRPC request to the remote membership service to ask for joining
	mslog(fmt.Sprintf("START SENDING LEAVE REQUEST to node '%s'.\n", targetNodeAddress))

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", targetNodeAddress, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to node %s due to error: %v\n", targetNodeAddress, err.Error())
		mslog(fmt.Sprintf("Cannot joining to membership service. Error: %s\n", errorMessage))
	}

	defer cc.Close()

	//gRPC call
	request := &fs533pb.LeaveRequest{
		LeavingNodeAddress:   leavingNodeAddress,
		RequesterNodeAddress: configuration.IPAddress,
	}
	requestSize := proto.Size(request)
	mslog(fmt.Sprintln("************** LEAVE REQUEST: size = ", requestSize))

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewMemberShipServiceClient(cc)

	leaveResponse, err := c.Leave(ctx, request)
	responseSize := proto.Size(leaveResponse)
	mslog(fmt.Sprintln("************** LEAVE RESPONSE: size = ", responseSize))

	if !leaveResponse.GetLeaveStatus() {
		mslog(fmt.Sprintf("Leaving request to target node %s has failed due to error: %s\n", targetNodeAddress, err))
	}

	mslog(fmt.Sprintln("Finish sending leave request to node ", targetNodeAddress))
	returnChan <- true
}

//sending joining request to a gateway node
//arguments
//-newJoinAddress: the address of the new join node
//-targetNodeAddress: the address of the gateway node that this request will send to
//-shouldSpreading: the option to indicate whether the gateway node should broadcast this new join to the other nodes or not
//returns
//-join status: indicate whether the join request succeeds or not
//-join error message: the error message to describe join failure reason
func sendJoiningRequest(newJoinAddress string, newJoinName string, targetNodeAddress string, shouldSpreading bool) (bool, []string, string, int32, []Fs533FileMetadata, []WriteTransaction) {
	//gRPC call
	request := &fs533pb.JoinRequest{
		NodeAddress:   newJoinAddress,
		NodeName:      newJoinName,
		NeedSpreading: shouldSpreading,
	}

	requestSize := proto.Size(request)

	//Sending rRPC request to the remote membership service to ask for joining
	mslog(fmt.Sprintf("************** JOIN REQUEST: for node '%s' to '%s', size =%d, spreading = %t.\n", newJoinAddress, targetNodeAddress, requestSize, shouldSpreading))

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", targetNodeAddress, configuration.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to node %s due to error: %v\n", targetNodeAddress, err.Error())
		mslog(fmt.Sprintf("Cannot joining to membership service. Error: %s\n", errorMessage))
		return false, []string{}, "", 0, nil, nil
	}

	defer cc.Close()

	//set a deadline to gRPC call to make sure that it will be timeout if the target agent failed by some reasons
	duration := time.Duration(configuration.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := fs533pb.NewMemberShipServiceClient(cc)

	joinResponse, err := c.Join(ctx, request)
	responseSize := proto.Size(joinResponse)
	mslog(fmt.Sprintln("************** JOIN RESPONSE: size = ", responseSize, ", response = ", joinResponse))

	if err != nil {
		mslog(fmt.Sprintln("Warning! It is unable process join for node ", newJoinAddress, " due to error ", err))
		return false, []string{}, "", 0, nil, nil
	}

	var initFs533Files []Fs533FileMetadata
	var initPendingWriteTransactions []WriteTransaction

	bpFs533Files := joinResponse.GetFs533Files()
	bpPendingWriteTransactions := joinResponse.GetPendingWriteTransactions()

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

	return true, joinResponse.GetMembers(), joinResponse.GetMasterNode(), joinResponse.GetElectionTerm(), initFs533Files, initPendingWriteTransactions
}

//this function is to print all the current status of membershipservice including its membership list and its status
//return
//- a string: prepresents all the information of the current membership service
func printMembershiplist() string {
	return fmt.Sprintf("len=%d, members = %v ... %v\n", membershipservice.activeMembers.len(), membershipservice.activeMembers.getIds(), membershipservice.activeMembers.getNames())
}

//start services including membership service and heartbeat service
func startServices() {
	//now it joins the membership group so enable MembershipStatus to 1
	membershipservice.membershipStatus = true
	membershipservice.needRestartSendingHeartBeats = false
	membershipservice.needRestartMonitoringHeartBeats = false

	go startMembershipService()

	//after joining, start sending heartbeat messages to its successors
	go startSendingHeartbeatMessages()

	//after joining, start monitoring heartbeat messages to its predecessors
	go startMonitoringHeartbeatMessages()
}

//start membership service which will handle join/leave requests
func startMembershipService() {
	//star membership service
	l, err := net.Listen("tcp", configuration.HostAddress)
	defer l.Close()

	if err != nil {
		log.Fatalf("Failed to listen: %v", err.Error())
	}

	mslog(fmt.Sprintf("Start membership service '%s' listening at '%s'...\n", configuration.InstanceID, configuration.HostAddress))
	mslog(fmt.Sprintf("Start file service '%s' listening at '%s'...\n", configuration.InstanceID, configuration.HostAddress))

	//opts := []grpc.ServerOption{}

	//grpcServer = grpc.NewServer(opts...)
	grpcServer = grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
		}),
	)

	fs533pb.RegisterMemberShipServiceServer(grpcServer, &msServer{})
	fs533pb.RegisterFileServiceServer(grpcServer, &grpcFileServer{})

	if err := grpcServer.Serve(l); err != nil {
		log.Fatalf("Failed to serve: %v", err.Error())
	}
}

//get member index on the active members list by its ip address value
//arguments
//-memberAddress: the ip address of the member
//returns
//-index: index of that member on the active members list
func getMemberIndex(memberAddress string) int {
	memberIndex := -1

	for i := 0; i < membershipservice.Len(); i++ {
		if membershipservice.activeMembers.getIds()[i] == memberAddress {
			memberIndex = i
			break
		}
	}

	return memberIndex
}

//this private function is to return total active members on the group
func (c *safeMembersCounter) len() int {
	c.mux.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	defer c.mux.Unlock()
	return len(c.activeMembersByAddress)
}

//this private function is to get the member id with offset position from index
func getMemberOffsetIndex(index int, offset int) int {
	newIndex := (index + offset + membershipservice.Len()) % membershipservice.Len()
	return newIndex
}

//this private function is to initialize membership group by addresses and names
func (c *safeMembersCounter) init(byAddress []string, byName []string) {
	c.mux.Lock()
	c.activeMembersByAddress = byAddress
	c.activeMembersByName = byName
	c.mux.Unlock()
}

// Value returns the current value of the counter for the given key.
func (c *safeMembersCounter) getIds() []string {
	c.mux.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	defer c.mux.Unlock()
	return c.activeMembersByAddress
}

// Value returns the current value of the counter for the given key.
func (c *safeMembersCounter) getNames() []string {
	c.mux.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	defer c.mux.Unlock()
	return c.activeMembersByName
}

//this function is to remove member from the current active members at a given index
//arguments
//-memberIndex: the index which will be removed
func (c *safeMembersCounter) removeMemberAtIndex(memberIndex int) {
	c.mux.Lock()
	removedNodeName := c.activeMembersByName[memberIndex]

	c.activeMembersByAddress = append(c.activeMembersByAddress[:memberIndex], c.activeMembersByAddress[memberIndex+1:]...)
	c.activeMembersByName = append(c.activeMembersByName[:memberIndex], c.activeMembersByName[memberIndex+1:]...)

	membershipservice.needRestartSendingHeartBeats = true
	membershipservice.needRestartMonitoringHeartBeats = true
	//hblog(fmt.Sprintf("Testing ... 4. membershipservice.needRestartSendingHeartBeats = %v. membershipservice.membershipStatus =%v", membershipservice.needRestartSendingHeartBeats, membershipservice.membershipStatus))

	mslog(fmt.Sprintf("************** REMOVE NODE '%s' at '%s' (previous index = %d). Active members: len = %d, [%s ..... %s]\n", removedNodeName, ConvertCurrentTimeToString(), memberIndex, len(c.activeMembersByAddress), c.activeMembersByAddress, c.activeMembersByName))

	c.mux.Unlock()
}

//this function is to append a new member to the current active members
//arguments
//-memberAddress: ip address of the new join
//-memberName: node name which includes node ip address and a timestamp of the new join
func (c *safeMembersCounter) addMember(memberAddress string, memberName string) {

	c.mux.Lock()

	c.activeMembersByAddress = append(c.activeMembersByAddress, memberAddress)
	c.activeMembersByName = append(c.activeMembersByName, memberName)

	if len(c.activeMembersByAddress) > 1 {
		membershipservice.needRestartSendingHeartBeats = true
		membershipservice.needRestartMonitoringHeartBeats = true

		mslog(fmt.Sprintf("************** ADD NEW JOIN ('%s','%s) at %s. Active members: len = %d, [%s ..... %s]\n", memberAddress, memberName, ConvertCurrentTimeToString(), len(c.activeMembersByAddress), c.activeMembersByAddress, c.activeMembersByName))
	}

	c.mux.Unlock()
}

//generate node name from node address which is attached by timestamp to distinguish between join and rejoin
//arguments
//-nodeAddress: node's ip address
func generateNodeName(nodeAddress string) string {
	return fmt.Sprintf("%s-%s", nodeAddress, ConvertCurrentTimeToString())
}

//log messages related to membership function.
func mslog(message string) {
	if configuration.HearbeatLog {
		log.Print(message)
	}
}
