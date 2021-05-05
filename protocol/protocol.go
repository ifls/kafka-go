package protocol

import (
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
)

// Message is an interface implemented by all request and response types of the
// kafka protocol.
//
// This interface is used mostly as a safe-guard to provide a compile-time check
// for values passed to functions dealing kafka message types.
type Message interface {
	ApiKey() ApiKey
}

type ApiKey int16

func (k ApiKey) String() string {
	if i := int(k); i >= 0 && i < len(apiNames) {
		return apiNames[i]
	}
	return strconv.Itoa(int(k))
}

func (k ApiKey) MinVersion() int16 { return k.apiType().minVersion() }

func (k ApiKey) MaxVersion() int16 { return k.apiType().maxVersion() }

func (k ApiKey) SelectVersion(minVersion, maxVersion int16) int16 {
	min := k.MinVersion()
	max := k.MaxVersion()
	switch {
	case min > maxVersion:
		return min
	case max < maxVersion:
		return max
	default:
		return maxVersion
	}
}

func (k ApiKey) apiType() apiType {
	if i := int(k); i >= 0 && i < len(apiTypes) {
		return apiTypes[i]
	}
	return apiType{}
}

const (
	Produce ApiKey = 0 // 发布消息 http://kafka.apache.org/protocol.html#The_Messages_Produce
	Fetch   ApiKey = 1 // 获取消息

	ListOffsets  ApiKey = 2 // 列举多个topic偏移量
	Metadata     ApiKey = 3 // 获取元数据
	OffsetCommit ApiKey = 8 // 偏移量提交
	OffsetFetch  ApiKey = 9 // 获取偏移量

	// http://zhongmingmao.me/2019/09/10/kafka-consumer-manage-tcp-connection/
	FindCoordinator  ApiKey = 10 // 当消费者程序首次启动调用poll方法时，需要向Kafka集群（集群中的任意Broker）发送FindCoordinator请求
	JoinGroup        ApiKey = 11 // 加入一个消费者组
	Heartbeat        ApiKey = 12 // 组成员存活心跳， 超时会被踢出
	LeaveGroup       ApiKey = 13 // 离开消费者组
	SyncGroup        ApiKey = 14 // 组长使用此同步请求向当前组中的所有成员进行状态(例如分区信息)变更
	DescribeGroups   ApiKey = 15 // 获取消费者组信息
	ListGroups       ApiKey = 16 // admin 列举所有消费者组
	SaslHandshake    ApiKey = 17 // Simple Authentication and Security Layer 握手认证
	SaslAuthenticate ApiKey = 36 // 和sasl握手有什么区别？？
	ApiVersions      ApiKey = 18 // 获取服务器支持的 指定 apikey的 版本
	CreateTopics     ApiKey = 19 // 创建 topic
	DeleteTopics     ApiKey = 20 // 删除topic

	// 下面这几个可能用的到
	// 配合消息的 sequence id 做 发布者消息幂等 https://news.51cto.com/art/202008/622979.htm
	InitProducerId              ApiKey = 22 // 生产者 client 向 broker 发起 InitProducerId request 请求一个 PID，后续发送的消息，都会带上这一个 PID 用于标明生产者的身份。
	DescribeConfigs             ApiKey = 32
	AlterConfigs                ApiKey = 33
	CreatePartitions            ApiKey = 37 // 创建分区
	ElectLeaders                ApiKey = 43 // 内部api，重新选举leader
	IncrementalAlterConfigs     ApiKey = 44
	AlterPartitionReassignments ApiKey = 45

	// 下面的没有用的
	LeaderAndIsr         ApiKey = 4  // 内部api 一个topic的指定partition的leader和isr，assignment replicas发生变动时，controller 向 broker发送的一种请求
	StopReplica          ApiKey = 5  // 停止复制
	UpdateMetadata       ApiKey = 6  // 更新元数据
	ControlledShutdown   ApiKey = 7  // kafka 服务器请求关闭下线
	DeleteRecords        ApiKey = 21 // 删除记录
	OffsetForLeaderEpoch ApiKey = 23 // 内部api broker 之间 同步offset

	// 这个请求是在producer生产消息前先要向TransactionCoordinator 发送一个AddParitionToTxn的请求将自己要发送的数据的topicParitition告诉transactionCoordinator
	AddPartitionsToTxn ApiKey = 24 // 参考 https://www.codenong.com/cs106266505/
	AddOffsetsToTxn    ApiKey = 25 // 如上， 事务相关
	EndTxn             ApiKey = 26 // 结束事务
	WriteTxnMarkers    ApiKey = 27 // 事务相关
	TxnOffsetCommit    ApiKey = 28

	DescribeAcls ApiKey = 29
	CreateAcls   ApiKey = 30
	DeleteAcls   ApiKey = 31

	AlterReplicaLogDirs ApiKey = 34
	DescribeLogDirs     ApiKey = 35

	CreateDelegationToken   ApiKey = 38
	RenewDelegationToken    ApiKey = 39
	ExpireDelegationToken   ApiKey = 40
	DescribeDelegationToken ApiKey = 41

	DeleteGroups               ApiKey = 42 //删除消费者组
	ListPartitionReassignments ApiKey = 46

	OffsetDelete ApiKey = 47 // 删除偏移量

	DescribeClientQuotas ApiKey = 48
	AlterClientQuotas    ApiKey = 49

	numApis = 50
)

var apiNames = [numApis]string{
	Produce:                     "Produce",
	Fetch:                       "Fetch",
	ListOffsets:                 "ListOffsets",
	Metadata:                    "Metadata",
	LeaderAndIsr:                "LeaderAndIsr",
	StopReplica:                 "StopReplica",
	UpdateMetadata:              "UpdateMetadata",
	ControlledShutdown:          "ControlledShutdown",
	OffsetCommit:                "OffsetCommit",
	OffsetFetch:                 "OffsetFetch",
	FindCoordinator:             "FindCoordinator",
	JoinGroup:                   "JoinGroup",
	Heartbeat:                   "Heartbeat",
	LeaveGroup:                  "LeaveGroup",
	SyncGroup:                   "SyncGroup",
	DescribeGroups:              "DescribeGroups",
	ListGroups:                  "ListGroups",
	SaslHandshake:               "SaslHandshake",
	ApiVersions:                 "ApiVersions",
	CreateTopics:                "CreateTopics",
	DeleteTopics:                "DeleteTopics",
	DeleteRecords:               "DeleteRecords",
	InitProducerId:              "InitProducerId",
	OffsetForLeaderEpoch:        "OffsetForLeaderEpoch",
	AddPartitionsToTxn:          "AddPartitionsToTxn",
	AddOffsetsToTxn:             "AddOffsetsToTxn",
	EndTxn:                      "EndTxn",
	WriteTxnMarkers:             "WriteTxnMarkers",
	TxnOffsetCommit:             "TxnOffsetCommit",
	DescribeAcls:                "DescribeAcls",
	CreateAcls:                  "CreateAcls",
	DeleteAcls:                  "DeleteAcls",
	DescribeConfigs:             "DescribeConfigs",
	AlterConfigs:                "AlterConfigs",
	AlterReplicaLogDirs:         "AlterReplicaLogDirs",
	DescribeLogDirs:             "DescribeLogDirs",
	SaslAuthenticate:            "SaslAuthenticate",
	CreatePartitions:            "CreatePartitions",
	CreateDelegationToken:       "CreateDelegationToken",
	RenewDelegationToken:        "RenewDelegationToken",
	ExpireDelegationToken:       "ExpireDelegationToken",
	DescribeDelegationToken:     "DescribeDelegationToken",
	DeleteGroups:                "DeleteGroups",
	ElectLeaders:                "ElectLeaders",
	IncrementalAlterConfigs:     "IncrementalAlterConfigs",
	AlterPartitionReassignments: "AlterPartitionReassignments",
	ListPartitionReassignments:  "ListPartitionReassignments",
	OffsetDelete:                "OffsetDelete",
	DescribeClientQuotas:        "DescribeClientQuotas",
	AlterClientQuotas:           "AlterClientQuotas",
}

type messageType struct {
	version  int16
	flexible bool
	gotype   reflect.Type
	decode   decodeFunc
	encode   encodeFunc
}

func (t *messageType) new() Message {
	return reflect.New(t.gotype).Interface().(Message)
}

type apiType struct {
	requests  []messageType
	responses []messageType
}

func (t apiType) minVersion() int16 {
	if len(t.requests) == 0 {
		return 0
	}
	return t.requests[0].version
}

func (t apiType) maxVersion() int16 {
	if len(t.requests) == 0 {
		return 0
	}
	return t.requests[len(t.requests)-1].version
}

var apiTypes [numApis]apiType

// Register is automatically called by sub-packages are imported to install a
// new pair of request/response message types.
func Register(req, res Message) {
	k1 := req.ApiKey()
	k2 := res.ApiKey()

	if k1 != k2 {
		panic(fmt.Sprintf("[%T/%T]: request and response API keys mismatch: %d != %d", req, res, k1, k2))
	}

	apiTypes[k1] = apiType{
		requests:  typesOf(req),
		responses: typesOf(res),
	}
}

func typesOf(v interface{}) []messageType {
	return makeTypes(reflect.TypeOf(v).Elem())
}

func makeTypes(t reflect.Type) []messageType {
	minVersion := int16(-1)
	maxVersion := int16(-1)

	// All future versions will be flexible (according to spec), so don't need to
	// worry about maxes here.
	minFlexibleVersion := int16(-1)

	forEachStructField(t, func(_ reflect.Type, _ index, tag string) {
		forEachStructTag(tag, func(tag structTag) bool {
			if minVersion < 0 || tag.MinVersion < minVersion {
				minVersion = tag.MinVersion
			}
			if maxVersion < 0 || tag.MaxVersion > maxVersion {
				maxVersion = tag.MaxVersion
			}
			if tag.TagID > -2 && (minFlexibleVersion < 0 || tag.MinVersion < minFlexibleVersion) {
				minFlexibleVersion = tag.MinVersion
			}
			return true
		})
	})

	types := make([]messageType, 0, (maxVersion-minVersion)+1)

	for v := minVersion; v <= maxVersion; v++ {
		flexible := minFlexibleVersion >= 0 && v >= minFlexibleVersion

		types = append(types, messageType{
			version:  v,
			gotype:   t,
			flexible: flexible,
			decode:   decodeFuncOf(t, v, flexible, structTag{}),
			encode:   encodeFuncOf(t, v, flexible, structTag{}),
		})
	}

	return types
}

type structTag struct {
	MinVersion int16
	MaxVersion int16
	Compact    bool
	Nullable   bool
	TagID      int
}

func forEachStructTag(tag string, do func(structTag) bool) {
	if tag == "-" {
		return // special case to ignore the field
	}

	forEach(tag, '|', func(s string) bool {
		tag := structTag{
			MinVersion: -1,
			MaxVersion: -1,

			// Legitimate tag IDs can start at 0. We use -1 as a placeholder to indicate
			// that the message type is flexible, so that leaves -2 as the default for
			// indicating that there is no tag ID and the message is not flexible.
			TagID: -2,
		}

		var err error
		forEach(s, ',', func(s string) bool {
			switch {
			case strings.HasPrefix(s, "min="):
				tag.MinVersion, err = parseVersion(s[4:])
			case strings.HasPrefix(s, "max="):
				tag.MaxVersion, err = parseVersion(s[4:])
			case s == "tag":
				tag.TagID = -1
			case strings.HasPrefix(s, "tag="):
				tag.TagID, err = strconv.Atoi(s[4:])
			case s == "compact":
				tag.Compact = true
			case s == "nullable":
				tag.Nullable = true
			default:
				err = fmt.Errorf("unrecognized option: %q", s)
			}
			return err == nil
		})

		if err != nil {
			panic(fmt.Errorf("malformed struct tag: %w", err))
		}

		if tag.MinVersion < 0 && tag.MaxVersion >= 0 {
			panic(fmt.Errorf("missing minimum version in struct tag: %q", s))
		}

		if tag.MaxVersion < 0 && tag.MinVersion >= 0 {
			panic(fmt.Errorf("missing maximum version in struct tag: %q", s))
		}

		if tag.MinVersion > tag.MaxVersion {
			panic(fmt.Errorf("invalid version range in struct tag: %q", s))
		}

		return do(tag)
	})
}

func forEach(s string, sep byte, do func(string) bool) bool {
	for len(s) != 0 {
		p := ""
		i := strings.IndexByte(s, sep)
		if i < 0 {
			p, s = s, ""
		} else {
			p, s = s[:i], s[i+1:]
		}
		if !do(p) {
			return false
		}
	}
	return true
}

func forEachStructField(t reflect.Type, do func(reflect.Type, index, string)) {
	for i, n := 0, t.NumField(); i < n; i++ {
		f := t.Field(i)

		if f.PkgPath != "" && f.Name != "_" {
			continue
		}

		kafkaTag, ok := f.Tag.Lookup("kafka")
		if !ok {
			kafkaTag = "|"
		}

		do(f.Type, indexOf(f), kafkaTag)
	}
}

func parseVersion(s string) (int16, error) {
	if !strings.HasPrefix(s, "v") {
		return 0, fmt.Errorf("invalid version number: %q", s)
	}
	i, err := strconv.ParseInt(s[1:], 10, 16)
	if err != nil {
		return 0, fmt.Errorf("invalid version number: %q: %w", s, err)
	}
	if i < 0 {
		return 0, fmt.Errorf("invalid negative version number: %q", s)
	}
	return int16(i), nil
}

func dontExpectEOF(err error) error {
	switch err {
	case nil:
		return nil
	case io.EOF:
		return io.ErrUnexpectedEOF
	default:
		return err
	}
}

type Broker struct {
	Rack string
	Host string
	Port int32
	ID   int32
}

func (b Broker) String() string {
	return net.JoinHostPort(b.Host, itoa(b.Port))
}

func (b Broker) Format(w fmt.State, v rune) {
	switch v {
	case 'd':
		io.WriteString(w, itoa(b.ID))
	case 's':
		io.WriteString(w, b.String())
	case 'v':
		io.WriteString(w, itoa(b.ID))
		io.WriteString(w, " ")
		io.WriteString(w, b.String())
		if b.Rack != "" {
			io.WriteString(w, " ")
			io.WriteString(w, b.Rack)
		}
	}
}

func itoa(i int32) string {
	return strconv.Itoa(int(i))
}

type Topic struct {
	Name       string
	Error      int16
	Partitions map[int32]Partition
}

type Partition struct {
	ID       int32
	Error    int16
	Leader   int32
	Replicas []int32
	ISR      []int32
	Offline  []int32
}

// BrokerMessage is an extension of the Message interface implemented by some
// request types to customize the broker assignment logic.
type BrokerMessage interface {
	// Given a representation of the kafka cluster state as argument, returns
	// the broker that the message should be routed to.
	Broker(Cluster) (Broker, error)
}

// GroupMessage is an extension of the Message interface implemented by some
// request types to inform the program that they should be routed to a group
// coordinator.
type GroupMessage interface {
	// Returns the group configured on the message.
	Group() string
}

// PreparedMessage is an extension of the Message interface implemented by some
// request types which may need to run some pre-processing on their state before
// being sent.
type PreparedMessage interface {
	// Prepares the message before being sent to a kafka broker using the API
	// version passed as argument.
	Prepare(apiVersion int16)
}

// Splitter is an interface implemented by messages that can be split into
// multiple requests and have their results merged back by a Merger.
type Splitter interface {
	// For a given cluster layout, returns the list of messages constructed
	// from the receiver for each requests that should be sent to the cluster.
	// The second return value is a Merger which can be used to merge back the
	// results of each request into a single message (or an error).
	Split(Cluster) ([]Message, Merger, error)
}

// Merger is an interface implemented by messages which can merge multiple
// results into one response.
type Merger interface {
	// Given a list of message and associated results, merge them back into a
	// response (or an error). The results must be either Message or error
	// values, other types should trigger a panic.
	Merge(messages []Message, results []interface{}) (Message, error)
}

// Result converts r to a Message or and error, or panics if r could be be
// converted to these types.
func Result(r interface{}) (Message, error) {
	switch v := r.(type) {
	case Message:
		return v, nil
	case error:
		return nil, v
	default:
		panic(fmt.Errorf("BUG: result must be a message or an error but not %T", v))
	}
}
