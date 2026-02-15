// Kafka consumer: reads from all 3 partitions, joins messages by ID into JSON.
// Message format: partition_N|id:X|ts:TIMESTAMP|name:NAME|task:TASK
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/IBM/sarama"
	"gopkg.in/yaml.v3"
)

const (
	broker = "127.0.0.1:9092"
	topic  = "teszt-partitioned"
)

type Config struct {
	Kafka struct {
		BootstrapServers string `yaml:"bootstrap_servers"`
		Topic            string `yaml:"topic"`
	} `yaml:"kafka"`
	Consumer struct {
		OutputFile    string `yaml:"output_file"`
		FlushEvery    int    `yaml:"flush_every"`
		LogEvery      int    `yaml:"log_every"`
		ConsumerGroup bool   `yaml:"consumer_group"`
		GroupID       string `yaml:"group_id"`
	} `yaml:"consumer"`
}

type partEntry struct {
	parsed *ParsedMsg
	msg    *sarama.ConsumerMessage
}

type ParsedMsg struct {
	Part int    `json:"-"`
	ID   int    `json:"-"`
	Ts   string `json:"ts"`
	Name string `json:"name"`
	Task string `json:"task"`
	Raw  string `json:"-"`
}

type HouseJSON struct {
	ID    int                  `json:"id"`
	House map[string]*ParsedMsg `json:"house"`
}

func parseMsg(raw string) (*ParsedMsg, error) {
	re := regexp.MustCompile(`partition_(\d+)\|id:(-?\d+)\|ts:([^|]+)\|name:([^|]+)\|task:([^|]+)`)
	m := re.FindStringSubmatch(raw)
	if m == nil {
		reOld := regexp.MustCompile(`part_(\d+)\|id:(-?\d+)\|ts:([^|]+)\|name:([^|]+)\|task:([^|]+)`)
		m = reOld.FindStringSubmatch(raw)
	}
	if m == nil {
		return nil, fmt.Errorf("unparseable: %s", raw)
	}
	var part, id int
	fmt.Sscanf(m[1], "%d", &part)
	fmt.Sscanf(m[2], "%d", &id)
	return &ParsedMsg{
		Part: part, ID: id, Ts: m[3], Name: m[4], Task: m[5], Raw: raw,
	}, nil
}

func loadConfig() (*Config, error) {
	cfgPath := filepath.Join("..", "config.yaml")
	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		cfgPath = "config.yaml"
	}
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if cfg.Consumer.OutputFile == "" {
		cfg.Consumer.OutputFile = "../output/houses.jsonl"
	}
	if cfg.Consumer.FlushEvery <= 0 {
		cfg.Consumer.FlushEvery = 100
	}
	if cfg.Consumer.LogEvery <= 0 {
		cfg.Consumer.LogEvery = 1000
	}
	if cfg.Consumer.GroupID == "" {
		cfg.Consumer.GroupID = "house-triplet-consumer"
	}
	return &cfg, nil
}

type offsetStorage struct {
	mu      sync.Mutex
	offsets map[int32]int64
	path    string
}

func (s *offsetStorage) load() map[int32]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		return nil
	}
	var raw map[string]int64
	if json.Unmarshal(data, &raw) != nil {
		return nil
	}
	out := make(map[int32]int64)
	for k, v := range raw {
		var p int32
		if _, err := fmt.Sscanf(k, "%d", &p); err == nil {
			out[p] = v
		}
	}
	return out
}

func (s *offsetStorage) update(partition int32, offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.offsets == nil {
		s.offsets = make(map[int32]int64)
	}
	if offset > s.offsets[partition] {
		s.offsets[partition] = offset
	}
}

// save writes atomically: temp file → sync → rename (crash-safe)
func (s *offsetStorage) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.offsets) == 0 {
		return nil
	}
	raw := make(map[string]int64)
	for k, v := range s.offsets {
		raw[fmt.Sprintf("%d", k)] = v
	}
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	f, err := os.Open(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		return err
	}
	// Sync may fail on Windows (antivirus, file lock) - continue with rename anyway
	_ = f.Sync()
	f.Close()
	if err := os.Rename(tmpPath, s.path); err != nil {
		os.Remove(tmpPath)
		// On Windows, rename over open file may fail; try direct write as fallback
		if err := os.WriteFile(s.path, data, 0644); err != nil {
			return err
		}
	}
	return nil
}

func runSimpleConsumer(cfg *Config, f *os.File, w *bufio.Writer, sigChan <-chan os.Signal) {
	bootstrap := cfg.Kafka.BootstrapServers
	if bootstrap == "" {
		bootstrap = broker
	}
	topicName := cfg.Kafka.Topic
	if topicName == "" {
		topicName = topic
	}

	offsetDir, _ := filepath.Abs(filepath.Dir(cfg.Consumer.OutputFile))
	offsetPath := filepath.Join(offsetDir, "consumer_offsets.json")
	offsetStore := &offsetStorage{path: offsetPath, offsets: make(map[int32]int64)}
	if loaded := offsetStore.load(); loaded != nil {
		offsetStore.offsets = loaded
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0

	consumer, err := sarama.NewConsumer([]string{bootstrap}, saramaConfig)
	if err != nil {
		log.Fatalf("Consumer: %v", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(topicName)
	if err != nil {
		log.Fatalf("Partitions: %v", err)
	}

	type partWithOffset struct {
		parsed *ParsedMsg
		offset int64
	}
	var tripletCount int64
	parts := make(map[int]map[int]*partWithOffset)
	var mu sync.Mutex

	for _, partition := range partitions {
		startOffset := sarama.OffsetOldest
		if o, ok := offsetStore.offsets[partition]; ok {
			startOffset = o + 1
		}
		pc, err := consumer.ConsumePartition(topicName, partition, startOffset)
		if err != nil {
			log.Printf("ConsumePartition %d: %v", partition, err)
			continue
		}
		defer pc.Close()

		go func(part int32) {
			for msg := range pc.Messages() {
				raw := string(msg.Value)
				parsed, err := parseMsg(raw)
				if err != nil {
					continue
				}
				mu.Lock()
				if parts[parsed.ID] == nil {
					parts[parsed.ID] = make(map[int]*partWithOffset)
				}
				parts[parsed.ID][parsed.Part] = &partWithOffset{parsed: parsed, offset: msg.Offset}
				if len(parts[parsed.ID]) == 3 {
					e1, e2, e3 := parts[parsed.ID][1], parts[parsed.ID][2], parts[parsed.ID][3]
					house := HouseJSON{
						ID: parsed.ID,
						House: map[string]*ParsedMsg{
							"partition_1": e1.parsed,
							"partition_2": e2.parsed,
							"partition_3": e3.parsed,
						},
					}
					jsonBytes, _ := json.Marshal(house)
					if _, err := w.Write(append(jsonBytes, '\n')); err != nil {
						log.Printf("Write error: %v", err)
						mu.Unlock()
						continue
					}
					offsetStore.update(0, e1.offset)
					offsetStore.update(1, e2.offset)
					offsetStore.update(2, e3.offset)
					cnt := atomic.AddInt64(&tripletCount, 1)
					if cnt%int64(cfg.Consumer.FlushEvery) == 0 {
						w.Flush()
						f.Sync()
						if err := offsetStore.save(); err != nil {
							log.Printf("Offset save error: %v", err)
						}
					}
					if cnt%int64(cfg.Consumer.LogEvery) == 0 {
						fmt.Printf("Progress: %d triplets written\n", cnt)
					}
					delete(parts, parsed.ID)
				}
				mu.Unlock()
			}
		}(partition)
	}

	resumeInfo := ""
	if len(offsetStore.offsets) > 0 {
		resumeInfo = " | Resuming from saved offsets"
	}
	fmt.Printf("Consumer connected. Topic: %s | Offsets: %s%s\n", topicName, offsetPath, resumeInfo)
	fmt.Printf("Output: %s | Offsets: %s | Flush every %d | Log every %d\n", cfg.Consumer.OutputFile, offsetPath, cfg.Consumer.FlushEvery, cfg.Consumer.LogEvery)
	fmt.Println("---")

	<-sigChan
	log.Println("Shutdown signal received, saving offsets and exiting...")
	if err := offsetStore.save(); err != nil {
		log.Printf("Offset save on shutdown: %v", err)
	}
}

type tripletHandler struct {
	cfg         *Config
	w           *bufio.Writer
	f           *os.File
	parts       map[int]map[int]*partEntry
	mu          sync.Mutex
	tripletCount int64
	ready       chan struct{}
	readyOnce   sync.Once
}

func (h *tripletHandler) Setup(sarama.ConsumerGroupSession) error {
	h.readyOnce.Do(func() { close(h.ready) })
	return nil
}

func (h *tripletHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *tripletHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
		raw := string(msg.Value)
		parsed, err := parseMsg(raw)
		if err != nil {
			session.MarkMessage(msg, "")
			continue
		}
		h.mu.Lock()
		if h.parts[parsed.ID] == nil {
			h.parts[parsed.ID] = make(map[int]*partEntry)
		}
		h.parts[parsed.ID][parsed.Part] = &partEntry{parsed: parsed, msg: msg}
		if len(h.parts[parsed.ID]) == 3 {
			e1, e2, e3 := h.parts[parsed.ID][1], h.parts[parsed.ID][2], h.parts[parsed.ID][3]
			house := HouseJSON{
				ID: parsed.ID,
				House: map[string]*ParsedMsg{
					"partition_1": e1.parsed,
					"partition_2": e2.parsed,
					"partition_3": e3.parsed,
				},
			}
			jsonBytes, _ := json.Marshal(house)
			if _, err := h.w.Write(append(jsonBytes, '\n')); err != nil {
				log.Printf("Write error: %v", err)
				h.mu.Unlock()
				continue
			}
			cnt := atomic.AddInt64(&h.tripletCount, 1)
			if cnt%int64(h.cfg.Consumer.FlushEvery) == 0 {
				h.w.Flush()
				h.f.Sync()
			}
			if cnt%int64(h.cfg.Consumer.LogEvery) == 0 {
				fmt.Printf("Progress: %d triplets written\n", cnt)
			}
			session.MarkMessage(e1.msg, "")
			session.MarkMessage(e2.msg, "")
			session.MarkMessage(e3.msg, "")
			delete(h.parts, parsed.ID)
		}
		h.mu.Unlock()
		}
	}
	return nil
}

func runConsumerGroup(cfg *Config, f *os.File, w *bufio.Writer, sigChan <-chan os.Signal) {
	bootstrap := cfg.Kafka.BootstrapServers
	if bootstrap == "" {
		bootstrap = broker
	}
	topicName := cfg.Kafka.Topic
	if topicName == "" {
		topicName = topic
	}
	groupID := cfg.Consumer.GroupID

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}

	handler := &tripletHandler{
		cfg:   cfg,
		w:     w,
		f:     f,
		parts: make(map[int]map[int]*partEntry),
		ready: make(chan struct{}),
	}

	client, err := sarama.NewConsumerGroup([]string{bootstrap}, groupID, saramaConfig)
	if err != nil {
		log.Fatalf("ConsumerGroup: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sigChan
		log.Println("Shutdown signal received, flushing and exiting...")
		cancel()
	}()

	fmt.Printf("Consumer (group) connected. Topic: %s | Group: %s | Offsets stored by Kafka\n", topicName, groupID)
	fmt.Printf("Output: %s | Flush every %d | Log every %d\n", cfg.Consumer.OutputFile, cfg.Consumer.FlushEvery, cfg.Consumer.LogEvery)
	fmt.Println("---")

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, []string{topicName}, handler); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Consume error: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	<-handler.ready
	wg.Wait()
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Config: %v", err)
	}

	os.MkdirAll(filepath.Dir(cfg.Consumer.OutputFile), 0755)
	f, err := os.OpenFile(cfg.Consumer.OutputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Output file: %v", err)
	}
	defer func() {
		f.Sync()
		f.Close()
	}()

	w := bufio.NewWriter(f)
	defer w.Flush()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	if cfg.Consumer.ConsumerGroup {
		runConsumerGroup(cfg, f, w, sigChan)
	} else {
		runSimpleConsumer(cfg, f, w, sigChan)
	}
}
