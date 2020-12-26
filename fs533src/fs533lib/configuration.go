package fs533lib

import (
	"fmt"
	"log"
	"net"

	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/tkanos/gonfig"
)

//Configuration ...Server settings
type Configuration struct {
	InstanceID                              string //instanceId of EC2 instance (a node)
	IPAddress                               string //instance's private address
	TCPPort                                 int    //TCP port which is used for listening to leave/join requests
	UDPPort                                 int    //UDP port which is used for listening to heartbeat messages
	LogPath                                 string //log files directory
	Fs533FilePath                           string //fs533 files directory
	LocalFilePath                           string //local files directory
	HostAddress                             string //host address which is IPAddress:TCPPort
	HeartbeatAddress                        string //heartbeat address which is IPAddress:UDPPort
	MaxDatagramSize                         int
	Timeout                                 int      //timeout for Join/Leave request
	HeartbeatIntervalTimeInMilliseconds     int      //interval time between 2 heartbeat messages
	HeartbeatTimeoutInMilliseconds          int      //timeout indicating that the node has failed
	GatewayNodes                            []string //3 leading nodes which are supposed that they will not fail at the same
	NumberOfPrecessorsAndSuccessors         int      //number of the predecessors and successors on the healthcheck topology ring
	NumberOfReplicas                        int      //number of preference nodes where storing replicate of a file
	WriteQuorum                             int      //write quorum
	ReadQuorum                              int      //read quorum
	FileChunkSize                           int      //chunk size for treaming file
	PendingWriteTransactionTimeoutInSeconds int      //a pending write transaction will be timeout if it reaches this total time
	NumberOfHeartbeatsShown                 int      //number of heartbeats which are printed out
	HearbeatLog                             bool     //turn on/off heartbeat log
	Membershiplog                           bool     //turn on/off membership log
}

//LoadConfiguration ... do
func LoadConfiguration(configFile string) Configuration {
	configuration := Configuration{}
	err := gonfig.GetConf(configFile, &configuration)

	if err != nil {
		log.Fatalf("Failed to load configuration file: %v", err.Error())
	}

	instanceID, ipAddress := loadAWSConfiguration()

	configuration.InstanceID = instanceID
	configuration.IPAddress = ipAddress
	configuration.HostAddress = fmt.Sprintf("%s:%d", configuration.IPAddress, configuration.TCPPort)
	configuration.HeartbeatAddress = fmt.Sprintf("%s:%d", configuration.IPAddress, configuration.UDPPort)

	return configuration
}

//Loading EC2 node's instanceId and private IP address
func loadAWSConfiguration() (instanceID string, ipAddress string) {
	//Load session from shared config
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := ec2metadata.New(sess)

	metadata, err := svc.GetInstanceIdentityDocument()

	if err != nil {
		//if it is unable to load ec2 metadata then using default configuration
		log.Println("Could not load current metadata", err.Error())
		return "node0", getOutboundIP()
	}

	return metadata.InstanceID, metadata.PrivateIP
}

func getOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	log.Println("local ip address = ", localAddr.IP.String())
	return localAddr.IP.String()
}
