package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/gogo/protobuf/proto"
	jsoniter "github.com/json-iterator/go"
	"github.com/nxsre/sshproxy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb"
	conf "github.com/woodliu/prometheusWriter/pkg/config"
	"github.com/woodliu/prometheusWriter/pkg/lib"
	"github.com/woodliu/prometheusWriter/pkg/wal"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
	"golang.org/x/net/proxy"
	"io"
	"log"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func envStr(name, fallback string) string {
	if s := os.Getenv(name); s != "" {
		return s
	}
	return fallback
}

var home = func() string {
	user, err := user.Current()
	if err == nil && user.HomeDir != "" {
		return user.HomeDir
	}

	return os.Getenv("HOME")
}()

var (
	sshKeyDir = envStr("HOS_KEY_DIR", filepath.Join(home, ".ssh"))
	sshKeys   = []string{
		filepath.Join(sshKeyDir, "id_rsa"),
		//filepath.Join(sshKeyDir, "id_ed25519"),
	}
	knownHosts = filepath.Join(sshKeyDir, "known_hosts")
)

func KfaConsumer(ctx context.Context, cfg *conf.Conf, msgChan chan<- []byte, es *tsdb.CircularExemplarStorage) {
	log.Println("开工")
	authMethods := sshproxy.ReadPrivateKeys(sshKeys...)
	if len(authMethods) == 0 {
		log.Println("no SSH keys found")
	}

	hostKeyCallback, err := knownhosts.New(knownHosts)
	if err != nil {
		log.Fatal(err)
	}

	p := sshproxy.NewProxy(ssh.ClientConfig{
		Timeout:         10 * time.Second,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
	})

	px := p.GetClient(sshproxy.Key{
		Host:     "192.168.109.7",
		Port:     31123,
		Username: "deployer",
		//Password: "xxxxxx",
	})

	//
	kafkaCfg := NewSaramaConfig()
	kafkaCfg.Net.Proxy = struct {
		Enable bool
		Dialer proxy.Dialer
	}{Enable: true, Dialer: px}
	kafkaCfg.ChannelBufferSize = 1000
	sarama.Logger = log.New(os.Stderr, "sarama", log.LstdFlags)

	c, err := sarama.NewConsumer(cfg.Kafka.Servers, kafkaCfg)
	if err != nil {
		log.Fatal(err)
	}

	var e io.Writer
	if cfg.Exemplars.Enable {
		//e = wal.NewEncoder(ctx, wal.WalDir, cfg.Exemplars.MaxExemplars)
		e, err = wal.LoadWal()
		if err != nil {
			log.Fatalln("wal", err)
		}
	}
	// 订阅的消息topic 列表，每个 topic 单独处理
	var allWg sync.WaitGroup
	for _, topic := range cfg.Kafka.Topics {
		allWg.Add(1)
		var (
			messages = make(chan *sarama.ConsumerMessage, 1)
			closing  = make(chan struct{})
			wg       sync.WaitGroup
		)

		go func() {
			for msg := range messages {
				flow := new(NetFlow)
				//fmt.Println(string(msg.Value))
				err := jsoniter.Unmarshal(msg.Value, &flow)
				if err != nil {
					log.Fatalln(err)
				}
				//log.Println(time.UnixMilli(flow.FlowTs * 1000).String())
				var value float64
				if flow.Direction == 1 {
					value = float64(flow.InboundBytes)
				} else if flow.Direction == 2 {
					value = float64(flow.OutboundBytes)
				}
				metrics := []*MetricPoint{
					&MetricPoint{Metric: flow.Protocol,
						TagsMap: map[string]interface{}{
							"direction": flow.Direction,
							"src_addr":  flow.SrcAddr,
							"src_port":  flow.SrcPort,
							"dst_addr":  flow.DstAddr,
							"dst_port":  flow.DstPort,
						},
						Time:  flow.FlowTs,
						Value: value},
				}
				if len(metrics) == 0 {
					return
				}

				ts := make([]prompb.TimeSeries, len(metrics))
				for i := range metrics {
					ts[i], err = convertOne(metrics[i])
					if err != nil {
						log.Fatalln(111, err)
						return
					}
				}
				wr, val, err := buildWriteRequest(ts)
				if err != nil {
					log.Fatalln(222, err)
					return
				}

				if nil != err {
					log.Fatalln(333, err)
				} else if nil != es {
					if cfg.Exemplars.Enable {
						e.Write(val)
						for _, v := range wr.Timeseries {
							lib.AddNewExemplar(es, v.Labels, v.Exemplars)
						}
					}
				}
				msgChan <- val
			}
		}()

		go func() {
			defer allWg.Done()

			go func() {
				signals := make(chan os.Signal, 1)
				signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
				<-signals
				log.Println("Initiating shutdown of consumer...")
				close(closing)
			}()

			partitions, err := getPartitions(c, "all", topic)
			if err != nil {
				log.Println("666666", err)
				return
			}
			for _, partition := range partitions {
				pc, err := c.ConsumePartition(topic, partition, sarama.OffsetNewest)
				if err != nil {
					log.Println("=====-----", err)
					return
				}
				go func(pc sarama.PartitionConsumer) {
					<-closing
					pc.AsyncClose()
				}(pc)
				wg.Add(1)
				go func(pc sarama.PartitionConsumer) {
					defer wg.Done()
					for message := range pc.Messages() {
						messages <- message
					}
				}(pc)
			}

			wg.Wait()
			log.Println("Done consuming topic", topic)
			close(messages)
		}()
	}
	log.Println("等待结束")
	allWg.Wait()
	log.Println("收工")
	if err := c.Close(); err != nil {
		log.Println("Failed to close consumer: ", err)
	}
}

func getPartitions(c sarama.Consumer, partitions, topic string) ([]int32, error) {
	if partitions == "all" {
		return c.Partitions(topic)
	}

	tmp := strings.Split(partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

func NewSaramaConfig() *sarama.Config {
	// NewConfig 设置了一些默认值
	c := sarama.NewConfig()
	c.ClientID = "xxx"

	c.Net.DialTimeout = 5 * time.Second
	c.Net.ReadTimeout = 5 * time.Second
	c.Net.WriteTimeout = 5 * time.Second
	c.Net.KeepAlive = 5 * time.Second

	// 刷新 metadata 的频率，默认为 10 分钟。见 Metadata.RefreshFrequency。
	// 不要获取整个集群的 metadata。一些集群有好几千 topic，获取完整 metadata 会很耗内存（观察到 Metadata 占了 30M 内存）
	c.Metadata.Full = false

	c.Producer.Timeout = 5 * time.Second

	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Consumer.Offsets.AutoCommit.Enable = true
	c.Consumer.Offsets.Retention = 7 * 24 * time.Hour
	c.Consumer.Fetch.Default = 2048 * 20480

	// 当需要处理的数据量大时，假如 Sarama 的 channel 缓冲区被填满（大小由 c.ChannelBufferSize 指定），Sarama 会在等待
	// c.Consumer.MaxProcessingTime 后暂断开该 partition 与 broker 的连接：
	// consumer/broker/2 abandoned subscription to shopee_inhouse_test__dwd_account_tab/0 because consuming was taking too long
	// 这里设置一个比较大的时间，以避免频繁与 broker 断开连接而影响消费速度。同时外部应该设置合理的 ChannelBufferSize。
	c.Consumer.MaxProcessingTime = 10 * time.Second

	// 处理 Source Config
	//c.Net.SASL.Enable = sc.SASL.Enable
	//c.Net.SASL.Version = sc.SASL.Version
	//c.Net.SASL.User = sc.SASL.User
	//c.Net.SASL.Password = sc.SASL.Password

	return c
}

type NetFlow struct {
	Direction        int    `json:"direction"`
	Layer            string `json:"layer"`
	SrcAddr          string `json:"src_addr"`
	DstAddr          string `json:"dst_addr"`
	SrcPort          uint32 `json:"src_port"`
	DstPort          uint32 `json:"dst_port"`
	Protocol         string `json:"protocol"`
	InboundBytes     uint64 `json:"inbound_bytes"`
	InboundPackets   uint64 `json:"inbound_packets"`
	InboundDuration  uint64 `json:"inbound_duration"`
	OutboundBytes    uint64 `json:"outbound_bytes"`
	OutboundPackets  uint64 `json:"outbound_packets"`
	OutboundDuration uint64 `json:"outbound_duration"`
	NodeID           string `json:"node_id"`
	NodeOamAddr      string `json:"node_oam_addr"`
	FlowTs           int64  `json:"flow_ts"`
	IfName           string `json:"if_name"`
}

type MetricPoint struct {
	Metric  string                 `json:"metric"` // 指标名称
	TagsMap map[string]interface{} `json:"tags"`   // 数据标签
	Time    int64                  `json:"time"`   // 时间戳，单位是秒
	Value   float64                `json:"value"`  // 内部字段，最终转换之后的float64数值
}

func convertOne(item *MetricPoint) (prompb.TimeSeries, error) {
	pt := prompb.TimeSeries{}
	pt.Samples = []prompb.Sample{{}}
	// Exemplars 需要服务端开启 --enable-feature=exemplar-storage
	pt.Exemplars = []prompb.Exemplar{{}}
	// Histograms 需要服务端开启 --enable-feature=native-histograms
	pt.Histograms = []prompb.Histogram{{}}
	s := sample{}
	s.t = item.Time
	s.v = item.Value
	// name
	if !MetricNameRE.MatchString(item.Metric) {
		return pt, errors.New("invalid metrics name")
	}
	nameLs := labels.Label{
		Name:  LABEL_NAME,
		Value: item.Metric,
	}
	s.labels = append(s.labels, nameLs)
	for k, v := range item.TagsMap {
		if model.LabelNameRE.MatchString(k) {
			ls := labels.Label{
				Name:  k,
				Value: fmt.Sprint(v),
			}
			s.labels = append(s.labels, ls)
		}
	}

	pt.Labels = labelsToLabelsProto(s.labels, pt.Labels)
	// 时间赋值问题,使用毫秒时间戳
	tsMs := time.Unix(s.t, 0).UnixNano() / 1e6
	pt.Samples[0].Timestamp = tsMs
	pt.Samples[0].Value = s.v

	pt.Exemplars[0].Value = s.v
	pt.Exemplars[0].Timestamp = tsMs
	pt.Exemplars[0].Labels = pt.Labels

	pt.Histograms[0].Timestamp = tsMs
	return pt, nil
}

func labelsToLabelsProto(labels labels.Labels, buf []prompb.Label) []prompb.Label {
	result := buf[:0]
	if cap(buf) < len(labels) {
		result = make([]prompb.Label, 0, len(labels))
	}
	for _, l := range labels {
		result = append(result, prompb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return result
}

const (
	LABEL_NAME = "__name__"
)

func buildWriteRequest(samples []prompb.TimeSeries) (wr *prompb.WriteRequest, data []byte, err error) {

	wr = &prompb.WriteRequest{
		Timeseries: samples,
		//Metadata: []prompb.MetricMetadata{
		//	prompb.MetricMetadata{
		//		Type:             prompb.MetricMetadata_GAUGE,
		//		MetricFamilyName: "xxxxx",
		//		Help:             "yyyyy",
		//		Unit:             "zzzz",
		//	},
		//},
	}
	data, err = proto.Marshal(wr)
	if err != nil {
		return
	}

	return
}

var MetricNameRE = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

type sample struct {
	labels labels.Labels
	t      int64
	v      float64
}
