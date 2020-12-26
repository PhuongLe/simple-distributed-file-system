package fs533lib

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	fs533pb "../fs533pb"
	"google.golang.org/protobuf/proto"
)

//MonitorPacket ...Heartbeat package
type MonitorPacket struct {
	NodeAddress      string
	LastTimeReceived time.Time
	Count            int32
}

//HeartbeatCounter ... is a struct to encapsulate all properties and functions related to membership
type safePredecessorsCounter struct {
	predecessors []MonitorPacket

	mux sync.Mutex
}

var predecessors safePredecessorsCounter
var enableSimulationMode bool
var messageDroppedRate float64

//update heartbeat value to a predecessor at index position
//arguments
//-index: the position of the predecessor
//-packet: the new heartbeat message for the above predecessor
func (c *safePredecessorsCounter) set(index int, packet MonitorPacket) {
	c.mux.Lock()
	c.predecessors[index].Count = packet.Count
	c.predecessors[index].NodeAddress = packet.NodeAddress
	c.predecessors[index].LastTimeReceived = time.Now()
	c.mux.Unlock()
}

//get heartbeat values of all the predecessors
func (c *safePredecessorsCounter) getValues() []MonitorPacket {
	c.mux.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	defer c.mux.Unlock()
	return c.predecessors
}

//get heartbeat value of the predecessor at position index
//argument
//-index: the position of the predecessor
//returns
//-monitorPacket: the heartbeat value of the predecessor at the input position
func (c *safePredecessorsCounter) get(index int) MonitorPacket {
	c.mux.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	defer c.mux.Unlock()
	return c.predecessors[index]
}

//update heartbeat count for the predecessor at a given index
//arguments
//-index: the position of the predecessor
//-hbCount: the new count value of the above predecessor
func (c *safePredecessorsCounter) update(index int, hbCount int32) {
	c.mux.Lock()
	c.predecessors[index].Count = hbCount
	c.predecessors[index].LastTimeReceived = time.Now()

	c.mux.Unlock()
}

//start sending heartbeat messages to the successors
func startSendingHeartbeatMessages() {
	hblog(fmt.Sprintln("Start sending heart beats..."))
	for {
		//if it is no longer a member of the group then stop sending heartbeat messages
		if !membershipservice.membershipStatus {
			hblog(fmt.Sprintln("Stop sending heartbeats since membership status is inactive now. 111111"))
			break
		}

		if membershipservice.Len() <= 1 {
			//if there is no successors, it should wait for a while before next checkup
			time.Sleep(time.Duration(configuration.HeartbeatIntervalTimeInMilliseconds) * time.Millisecond)
			continue
		}

		membershipservice.needRestartSendingHeartBeats = false
		//hblog(fmt.Sprintf("Testing ... 2. membershipservice.needRestartSendingHeartBeats = %v. membershipservice.membershipStatus =%v", membershipservice.needRestartSendingHeartBeats, membershipservice.membershipStatus))

		//if activeMembersByAddress is greater than 2 nodes, send heartbeat to its 1st successor
		memberIndex := getMemberIndex(configuration.IPAddress)
		c := make(chan bool)
		activeMembersByAddressCount := membershipservice.Len()
		numberOfSuccessors := 0

		for i := 0; i < configuration.NumberOfPrecessorsAndSuccessors; i++ {
			if activeMembersByAddressCount < i+2 {
				break
			}
			nextMemberIndex := getMemberOffsetIndex(memberIndex, 1+i)
			hblog(fmt.Sprintf("Start sending heartbeats to node %d at '%s', every %d milliseconds.\n", i+1, membershipservice.activeMembers.getIds()[nextMemberIndex], configuration.HeartbeatIntervalTimeInMilliseconds))
			go sendHeartBeat(membershipservice.activeMembers.getIds()[nextMemberIndex], int64(i), c)

			numberOfSuccessors++
		}

		for i := 0; i < numberOfSuccessors; i++ {
			<-c
		}

		if membershipservice.membershipStatus {
			hblog(fmt.Sprintln("Restart sending heartbeats since active members list has changed."))
		}
	}
}

//send heartbeat message to the tartget node
//arguments
//-tartgetNodeAddress: the ip address of the successor node which will receive heartbeat message
//-nodeSeed: the random seed for failure simulation
func sendHeartBeat(targetNodeAddress string, nodeSeed int64, c chan bool) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", targetNodeAddress, configuration.UDPPort))
	checkError(err)

	conn, err := net.DialUDP("udp", nil, addr)

	defer conn.Close()

	count := 0
	countTo50 := 0
	droppedMessagesList := []int{}

	for {

		//if message dropped simulation mode is enabled, it will try to drop a random message by input rate
		if enableSimulationMode && countTo50 == 0 {
			droppedMessagesList = generateRandomDroppedMessagesList(nodeSeed)
		}

		//if simulation mode is enabled and message number is indicated as a dropped one, just simply ignore this turn
		messageIsDropped := shouldDropMessage(droppedMessagesList, countTo50)
		if !enableSimulationMode || !messageIsDropped {
			//loop sending heartbeat message forever, till active member changes?
			hbPacket := &fs533pb.HeartbeatPacket{HbCount: int32(count), SentTime: time.Now().Unix()}
			data, err := proto.Marshal(hbPacket)
			if err != nil {
				hblog(fmt.Sprintln("marshalling error: ", err))
			}

			buf := []byte(data)

			if hbPacket.GetHbCount() < int32(configuration.NumberOfHeartbeatsShown) {
				hblog(fmt.Sprintf("Send heartbeat to node '%s' at '%s', count = %d", addr.String(), ConvertCurrentTimeToString(), hbPacket.GetHbCount()))
			}

			if count == 0 {
				hblog(fmt.Sprintln("************** HEARTBEAT PACKET: size = ", len(data)))
			}
			_, err = conn.Write(buf)
			if err != nil {
				hblog(fmt.Sprintln("Error :", err))
				//hblog(fmt.Sprintf("Testing ... 3. sendHeartBeat() membershipservice.needRestartSendingHeartBeats = %v. membershipservice.membershipStatus =%v", membershipservice.needRestartSendingHeartBeats, membershipservice.membershipStatus))

				if membershipservice.needRestartSendingHeartBeats || !membershipservice.membershipStatus {
					hblog(fmt.Sprintf("Stop sending heartbeat to node '%s' because membership has changed.  membershipservice.needRestartSendingHeartBeats = %v. membershipservice.membershipStatus =%v", targetNodeAddress, membershipservice.needRestartSendingHeartBeats, membershipservice.membershipStatus))
					c <- true
					break
				}
			}
		}

		if enableSimulationMode && messageIsDropped {
			hblog(fmt.Sprintf("************** DROP HEARTBEAT PACKET '%d' to node %s (%d/50) \n", count, addr.String(), countTo50))
		}

		count = count + 1
		countTo50 = (countTo50 + 1) % 50

		time.Sleep(time.Duration(configuration.HeartbeatIntervalTimeInMilliseconds) * time.Millisecond)
		if membershipservice.needRestartSendingHeartBeats {
			//hblog(fmt.Sprintf("Testing ... 5. Stop sending heartbeat to node '%s' because membership has changed.  membershipservice.needRestartSendingHeartBeats = %v. membershipservice.membershipStatus =%v", targetNodeAddress, membershipservice.needRestartSendingHeartBeats, membershipservice.membershipStatus))
			c <- true
			break
		}
	}
}

//check if a message should be dropped or not. It should be dropped if its order is on the droppedmessagelist
//arguments
//-droppedMessagesList: the list of messages orders that should be dropped
//-messageNo: the message order which is checked whether it should be dropped or not
//returns
//-bool: returns true if the message should be dropped. Otherwise, returns false
func shouldDropMessage(droppedMessagesList []int, messageNo int) bool {
	for _, i := range droppedMessagesList {
		if i == messageNo {
			return true
		}
	}
	return false
}

//generate a list of random message orders which should should be dropped
//arguments
//-nodeSeed: a random seed to generate random orders
//returns
//-[]int: an array of message orders which should be dropped
func generateRandomDroppedMessagesList(nodeSeed int64) []int {
	droppedMessagesList := []int{}

	seed := nodeSeed + time.Now().UnixNano()
	s1 := rand.NewSource(seed)
	r1 := rand.New(s1)

	//generate a random number which is to specify which message should be dropped
	switch messageDroppedRate {
	case 0.1, 0.3:
		//random generate 5 number less than 10
		for j := 0; j < 5; j++ {
			for i := 0; i < int(messageDroppedRate*10); i++ {
				n := r1.Intn(10)
				m := n + j*10
				droppedMessagesList = append(droppedMessagesList, m)
			}
		}
	default:
		//for any other rates, it will be treated as rate 0.02
		for i := 0; i < 2; i++ {
			n := r1.Intn(50)
			droppedMessagesList = append(droppedMessagesList, n)
		}
	}

	return droppedMessagesList
}

//start monitoring heartbeat messages from the predecessors
func startMonitoringHeartbeatMessages() {
	hblog(fmt.Sprintln("Start mornitoring heart beats..."))

	membershipservice.needRestartMonitoringHeartBeats = false

	initializePredecessors()

	c := make(chan bool)

	go listenHeartBeats(c)

	for {

		if !membershipservice.membershipStatus {
			hblog(fmt.Sprintln("Stop monitoring heartbeats since membership status is inactive now. 2222"))
			<-c
			hblog(fmt.Sprintln("Stoped monitoring service"))
			break
		}

		if membershipservice.needRestartMonitoringHeartBeats {
			if membershipservice.Len() >= 2 {
				hblog(fmt.Sprintln("Restart monitoring heartbeats since members list has changed. 33333"))
			}
			<-c

			membershipservice.needRestartMonitoringHeartBeats = false

			c = make(chan bool)
			go listenHeartBeats(c)
		}

		if membershipservice.Len() <= 1 {
			time.Sleep(time.Duration(configuration.HeartbeatIntervalTimeInMilliseconds) * time.Millisecond)
			continue
		}

		countNo := 0
		for idx, predecessor := range predecessors.getValues() {
			if predecessor.NodeAddress == "" {
				continue
			}

			if hasFailed(predecessor) {
				countNo++
				notifyFailure(predecessor.NodeAddress)
				predecessors.set(idx, MonitorPacket{})
			}
		}

		time.Sleep(time.Duration(configuration.HeartbeatIntervalTimeInMilliseconds) * time.Millisecond)
	}
}

//reset predecessors list
func initializePredecessors() {
	size := configuration.NumberOfPrecessorsAndSuccessors
	initPackets := []MonitorPacket{}
	for idx := 0; idx < size; idx++ {
		packet := MonitorPacket{
			NodeAddress:      "",
			LastTimeReceived: time.Time{},
			Count:            0,
		}
		initPackets = append(initPackets, packet)
	}
	predecessors = safePredecessorsCounter{predecessors: initPackets}
}

//listen to heartbeat message. This function will update latest heartbeat count to the appropriate predecessor or alert a failure
func listenHeartBeats(c chan bool) {
	if membershipservice.Len() <= 1 {
		c <- true
		return
	}

	addr, err := net.ResolveUDPAddr("udp", configuration.HeartbeatAddress)
	checkError(err)
	l, err := net.ListenUDP("udp", addr)
	checkError(err)
	//defer l.Close()
	memberIndex := getMemberIndex(configuration.IPAddress)

	activeMembersByAddressCount := membershipservice.Len()

	initializePredecessors()

	for i := 0; i < configuration.NumberOfPrecessorsAndSuccessors; i++ {
		if activeMembersByAddressCount < i+2 {
			break
		}
		previousMemberIndex := getMemberOffsetIndex(memberIndex, -1*(i+1))

		predecessors.set(i, MonitorPacket{
			NodeAddress:      membershipservice.activeMembers.getIds()[previousMemberIndex],
			LastTimeReceived: time.Now(),
			Count:            0,
		})
		hblog(fmt.Sprintf("Start listening to predecessor '%d' from '%s' at '%s'. \n", i+1, predecessors.get(i).NodeAddress, predecessors.get(i).LastTimeReceived.Format(dateTimeFormat)))
	}

	for {
		if !membershipservice.membershipStatus {
			//if membership status is inactive, stop the listener
			hblog(fmt.Sprintln("Stop monitoring heartbeats since membership status is inactive now. 1111"))
			l.Close()
			c <- true
			return
		}

		if membershipservice.needRestartMonitoringHeartBeats {
			hblog(fmt.Sprintf("Restart monitoring heartbeats since active members list has changed.2222"))
			l.Close()
			c <- true
			return
		}

		buf := make([]byte, 1024)

		err := l.SetReadDeadline(time.Now().Add(3 * time.Second))
		if err != nil {
			hblog(fmt.Sprintln("Error when set read deadline", err))
		}

		byteSize, remoteAddr, err := l.ReadFromUDP(buf)

		if err != nil {
			hblog(fmt.Sprintln("Uncaught error while listening to heartbeat. Connection is closed. ", err))
			l.Close()
			c <- true
			return
		}
		hbPacket := &fs533pb.HeartbeatPacket{}
		err = proto.Unmarshal(buf[0:byteSize], hbPacket)
		if hbPacket.GetHbCount() < int32(configuration.NumberOfHeartbeatsShown) {
			hblog(fmt.Sprintf("Receive heartbeat from '%s', at '%s', count =  %d.\n", remoteAddr.String(), time.Unix((*hbPacket).GetSentTime(), 0).Format(dateTimeFormat), (*hbPacket).GetHbCount()))
		}

		if err != nil {
			hblog(fmt.Sprintln("Monitoring heartbeat process has failed due to unknown error:", err))
			c <- true
			l.Close()
		} else {
			for idx, predecessor := range predecessors.getValues() {
				if predecessor.NodeAddress == remoteAddr.IP.String() {
					predecessors.update(idx, hbPacket.GetHbCount())
				}
			}
		}
	}
}

//examine if the predecessor has failed or not by checking its latest heartbeat packet's time received with current time.
//it is alerted as failed if the duration is greater than 1000 milliseconds
func hasFailed(monitoredPacket MonitorPacket) bool {
	if monitoredPacket.NodeAddress == "" {
		return false
	}

	diff := time.Now().Sub(monitoredPacket.LastTimeReceived)
	diffInMilliseconds := int64(diff / time.Millisecond)
	if diffInMilliseconds > int64(configuration.HeartbeatTimeoutInMilliseconds) {
		hblog(fmt.Sprintf("************** DETECT FAILURE of node '%s' at '%s', last time received heartbeat is '%s', count = %d, diff = %d", monitoredPacket.NodeAddress, time.Now().Format(dateTimeFormat), monitoredPacket.LastTimeReceived.Format(time.RFC3339), monitoredPacket.Count, diffInMilliseconds))
		return true
	}
	return false
}

//notify about the failure of a predecessor at a given address
//arguments
//-failureNodeAddress: the address of node failed
func notifyFailure(failureNodeAddress string) {
	hblog(fmt.Sprintln("Start notifying other nodes about failure of :", failureNodeAddress))
	failureNodeIndex := getMemberIndex(failureNodeAddress)
	membershipservice.activeMembers.removeMemberAtIndex(failureNodeIndex)

	//start leader election if the failed node is master node
	if failureNodeAddress == masterNode && membershipservice.Len() > 1 {
		fileServer.StartLeaderElectionProcess()
	}

	//start broadcasting failure alert to all the other nodes
	c := make(chan bool)
	count := 0
	for _, member := range membershipservice.activeMembers.getIds() {
		if configuration.IPAddress == member {
			//if the node detecting failure is the master node, it will execute firenodeleave event beside of removing the failed node from members list
			if configuration.IPAddress == masterNode {
				fileServer.FireNodeLeave(failureNodeAddress)
			}
			continue
		}
		count++
		go sendLeavingRequest(failureNodeAddress, member, c)
	}

	for i := 0; i < count; i++ {
		<-c
	}
}

//show error if there is
func checkError(err error) {
	if err != nil {
		hblog(fmt.Sprintln("Error: ", err))
	}
}

//log messages related to heartbeat function.
func hblog(message string) {
	if configuration.HearbeatLog {
		log.Print(message)
	}
}
