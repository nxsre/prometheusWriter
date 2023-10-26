package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/nxsre/sshproxy"
	conf "github.com/woodliu/prometheusWriter/pkg/config"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
	"golang.org/x/net/proxy"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
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
	c.Consumer.MaxProcessingTime = 1 * time.Second

	// 处理 Source Config
	//c.Net.SASL.Enable = sc.SASL.Enable
	//c.Net.SASL.Version = sc.SASL.Version
	//c.Net.SASL.User = sc.SASL.User
	//c.Net.SASL.Password = sc.SASL.Password

	return c
}

// 本功能仅用于测试使用
func main() {
	cfg, err := conf.Load("./config.json")
	if err != nil {
		log.Fatal(err.Error())
	}

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

	kcfg := NewSaramaConfig()
	kcfg.Net.Proxy = struct {
		Enable bool
		Dialer proxy.Dialer
	}{Enable: true, Dialer: px}
	c, err := sarama.NewConsumer(cfg.Kafka.Servers, kcfg)
	if err != nil {
		log.Fatal(err)
	}
	// 订阅的消息topic 列表
	for _, topic := range cfg.Kafka.Topics {
		partitions, err := getPartitions(c, "all", topic)
		if err != nil {
			log.Fatalln(err)
			continue
		}
		for _, partition := range partitions {
			consumer, err := c.ConsumePartition(topic, partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatalln(err)
				continue
			}

			msgs := consumer.Messages()
			for {
				msg := <-msgs
				if err == nil {
					fmt.Printf("Message on %d: %s\n", msg.Partition, string(msg.Value))
				} else {
					// 客户端将自动尝试恢复所有的 error
					fmt.Printf("Consumer error: %v (%v)\n", err, msg)
				}
			}
		}

	}

	c.Close()
}
