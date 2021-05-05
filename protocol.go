package kafka

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

type ApiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

func (v ApiVersion) Format(w fmt.State, r rune) {
	switch r {
	case 's':
		fmt.Fprint(w, apiKey(v.ApiKey))
	case 'd':
		switch {
		case w.Flag('-'):
			fmt.Fprint(w, v.MinVersion)
		case w.Flag('+'):
			fmt.Fprint(w, v.MaxVersion)
		default:
			fmt.Fprint(w, v.ApiKey)
		}
	case 'v':
		switch {
		case w.Flag('-'):
			fmt.Fprintf(w, "v%d", v.MinVersion)
		case w.Flag('+'):
			fmt.Fprintf(w, "v%d", v.MaxVersion)
		case w.Flag('#'):
			fmt.Fprintf(w, "kafka.ApiVersion{ApiKey:%d MinVersion:%d MaxVersion:%d}", v.ApiKey, v.MinVersion, v.MaxVersion)
		default:
			fmt.Fprintf(w, "%s[v%d:v%d]", apiKey(v.ApiKey), v.MinVersion, v.MaxVersion)
		}
	}
}

type apiKey int16

const (
	produce     apiKey = 0
	fetch       apiKey = 1
	listOffsets apiKey = 2
	metadata    apiKey = 3

	// 参考 https://blog.csdn.net/qq_37502106/article/details/80271800
	// kafka不是完全同步，也不是完全异步，是一种ISR机制： 1. leader会维护一个与其基本保持同步的Replica列表，该列表称为ISR(in-sync Replica)
	leaderAndIsr                apiKey = 4 // 内部api， client没用到
	stopReplica                 apiKey = 5 // client没用到
	updateMetadata              apiKey = 6 // client没用到
	controlledShutdown          apiKey = 7 // client没用到
	offsetCommit                apiKey = 8
	offsetFetch                 apiKey = 9
	findCoordinator             apiKey = 10
	joinGroup                   apiKey = 11
	heartbeat                   apiKey = 12
	leaveGroup                  apiKey = 13
	syncGroup                   apiKey = 14
	describeGroups              apiKey = 15
	listGroups                  apiKey = 16
	saslHandshake               apiKey = 17
	apiVersions                 apiKey = 18
	createTopics                apiKey = 19
	deleteTopics                apiKey = 20
	deleteRecords               apiKey = 21 // client没用到
	initProducerId              apiKey = 22 // client没用到
	offsetForLeaderEpoch        apiKey = 23 // client没用到
	addPartitionsToTxn          apiKey = 24 // client没用到
	addOffsetsToTxn             apiKey = 25 // client没用到
	endTxn                      apiKey = 26 // client没用到
	writeTxnMarkers             apiKey = 27 // client没用到
	txnOffsetCommit             apiKey = 28 // client没用到
	describeAcls                apiKey = 29 // client没用到
	createAcls                  apiKey = 30 // client没用到
	deleteAcls                  apiKey = 31 // client没用到
	describeConfigs             apiKey = 32 // client没用到
	alterConfigs                apiKey = 33 // client没用到
	alterReplicaLogDirs         apiKey = 34 // client没用到
	describeLogDirs             apiKey = 35 // client没用到
	saslAuthenticate            apiKey = 36
	createPartitions            apiKey = 37 // client没用到
	createDelegationToken       apiKey = 38 // client没用到
	renewDelegationToken        apiKey = 39 // client没用到
	expireDelegationToken       apiKey = 40 // client没用到
	describeDelegationToken     apiKey = 41 // client没用到
	deleteGroups                apiKey = 42 // client没用到
	electLeaders                apiKey = 43 // client没用到
	incrementalAlterConfigs     apiKey = 44 // client没用到
	alterPartitionReassignments apiKey = 45 // client没用到
	listPartitionReassignments  apiKey = 46 // client没用到
	offsetDelete                apiKey = 47 // client没用到
)

func (k apiKey) String() string {
	if i := int(k); i >= 0 && i < len(apiKeyStrings) {
		return apiKeyStrings[i]
	}
	return strconv.Itoa(int(k))
}

type apiVersion int16

const (
	v0  = 0
	v1  = 1
	v2  = 2
	v3  = 3
	v4  = 4
	v5  = 5
	v6  = 6
	v7  = 7
	v8  = 8
	v9  = 9
	v10 = 10
	// 最新的 已经有v11了
)

var apiKeyStrings = [...]string{
	produce:                     "Produce",
	fetch:                       "Fetch",
	listOffsets:                 "ListOffsets",
	metadata:                    "Metadata",
	leaderAndIsr:                "LeaderAndIsr",
	stopReplica:                 "StopReplica",
	updateMetadata:              "UpdateMetadata",
	controlledShutdown:          "ControlledShutdown",
	offsetCommit:                "OffsetCommit",
	offsetFetch:                 "OffsetFetch",
	findCoordinator:             "FindCoordinator",
	joinGroup:                   "JoinGroup",
	heartbeat:                   "Heartbeat",
	leaveGroup:                  "LeaveGroup",
	syncGroup:                   "SyncGroup",
	describeGroups:              "DescribeGroups",
	listGroups:                  "ListGroups",
	saslHandshake:               "SaslHandshake",
	apiVersions:                 "ApiVersions",
	createTopics:                "CreateTopics",
	deleteTopics:                "DeleteTopics",
	deleteRecords:               "DeleteRecords",
	initProducerId:              "InitProducerId",
	offsetForLeaderEpoch:        "OffsetForLeaderEpoch",
	addPartitionsToTxn:          "AddPartitionsToTxn",
	addOffsetsToTxn:             "AddOffsetsToTxn",
	endTxn:                      "EndTxn",
	writeTxnMarkers:             "WriteTxnMarkers",
	txnOffsetCommit:             "TxnOffsetCommit",
	describeAcls:                "DescribeAcls",
	createAcls:                  "CreateAcls",
	deleteAcls:                  "DeleteAcls",
	describeConfigs:             "DescribeConfigs",
	alterConfigs:                "AlterConfigs",
	alterReplicaLogDirs:         "AlterReplicaLogDirs",
	describeLogDirs:             "DescribeLogDirs",
	saslAuthenticate:            "SaslAuthenticate",
	createPartitions:            "CreatePartitions",
	createDelegationToken:       "CreateDelegationToken",
	renewDelegationToken:        "RenewDelegationToken",
	expireDelegationToken:       "ExpireDelegationToken",
	describeDelegationToken:     "DescribeDelegationToken",
	deleteGroups:                "DeleteGroups",
	electLeaders:                "ElectLeaders",
	incrementalAlterConfigs:     "IncrementalAlfterConfigs",
	alterPartitionReassignments: "AlterPartitionReassignments",
	listPartitionReassignments:  "ListPartitionReassignments",
	offsetDelete:                "OffsetDelete",
}

type requestHeader struct {
	Size          int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	ClientID      string
}

func (h requestHeader) size() int32 {
	return 4 + 2 + 2 + 4 + sizeofString(h.ClientID)
}

func (h requestHeader) writeTo(wb *writeBuffer) {
	wb.writeInt32(h.Size)
	wb.writeInt16(h.ApiKey)
	wb.writeInt16(h.ApiVersion)
	wb.writeInt32(h.CorrelationID)
	wb.writeString(h.ClientID)
}

// todo 参考 http://kafka.apache.org/protocol.html#protocol_details 协议文件
type request interface {
	size() int32
	writable
}

func makeInt8(b []byte) int8 {
	return int8(b[0])
}

func makeInt16(b []byte) int16 {
	return int16(binary.BigEndian.Uint16(b))
}

func makeInt32(b []byte) int32 {
	return int32(binary.BigEndian.Uint32(b))
}

func makeInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

func expectZeroSize(sz int, err error) error {
	if err == nil && sz != 0 {
		err = fmt.Errorf("reading a response left %d unread bytes", sz)
	}
	return err
}
