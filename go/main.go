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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

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
		OutputFile         string `yaml:"output_file"`
		FlushEvery         int    `yaml:"flush_every"`
		LogEvery           int    `yaml:"log_every"`
		WriteBufferSize    int    `yaml:"write_buffer_size"`
		ConsumerGroup      bool   `yaml:"consumer_group"`
		GroupID            string `yaml:"group_id"`
		StornoTopic        string `yaml:"storno_topic"`
		StornoFile         string `yaml:"storno_file"`
		StornoOutputFile   string `yaml:"storno_output_file"`
		StornoFlushEvery   int    `yaml:"storno_flush_every"`
		HouseChanBuffer    int    `yaml:"house_chan_buffer"`
		MaxPendingTriplets int    `yaml:"max_pending_triplets"`
		FetchMinBytes           int `yaml:"fetch_min_bytes"`
		FetchMaxBytes           int `yaml:"fetch_max_bytes"`
		OffsetSaveIntervalSec   int `yaml:"offset_save_interval_sec"` // időzített mentés (0 = kikapcsolva)
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

type PartitionOut struct {
	Ts     string `json:"ts"`
	Name   string `json:"name"`
	Task   string `json:"task"`
	Offset int64  `json:"offset"`
}

type HouseJSON struct {
	ID    int                     `json:"id"`
	House map[string]*PartitionOut `json:"house"`
}

// StornoHouseJSON: house written to storno_houses.jsonl when partition(s) removed
type StornoHouseJSON struct {
	ID                 int                     `json:"id"`
	House              map[string]*PartitionOut `json:"house"`
	Storno             string                  `json:"storno"`              // "full" or "partial"
	ExcludedPartitions []int                   `json:"excluded_partitions,omitempty"`
}

func parseMsg(raw string) (*ParsedMsg, error) {
	parts := strings.Split(raw, "|")
	if len(parts) < 5 {
		return nil, fmt.Errorf("unparseable: %s", raw)
	}
	var part, id int64
	if strings.HasPrefix(parts[0], "partition_") {
		part, _ = strconv.ParseInt(parts[0][10:], 10, 64)
	} else if strings.HasPrefix(parts[0], "part_") {
		part, _ = strconv.ParseInt(parts[0][5:], 10, 64)
	} else {
		return nil, fmt.Errorf("unparseable: %s", raw)
	}
	if !strings.HasPrefix(parts[1], "id:") {
		return nil, fmt.Errorf("unparseable: %s", raw)
	}
	id, _ = strconv.ParseInt(parts[1][3:], 10, 64)
	return &ParsedMsg{
		Part: int(part), ID: int(id), Ts: parts[2], Name: parts[3], Task: parts[4], Raw: raw,
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
	if cfg.Consumer.GroupID == "" || cfg.Consumer.GroupID == "hostname" {
		if h, err := os.Hostname(); err == nil && h != "" {
			cfg.Consumer.GroupID = h
		} else {
			cfg.Consumer.GroupID = "house-triplet-consumer"
		}
	}
	if cfg.Consumer.StornoTopic == "" {
		cfg.Consumer.StornoTopic = "house-storno"
	}
	if cfg.Consumer.StornoOutputFile == "" {
		cfg.Consumer.StornoOutputFile = filepath.Join(filepath.Dir(cfg.Consumer.OutputFile), "storno_houses.jsonl")
	}
	if cfg.Consumer.HouseChanBuffer <= 0 {
		cfg.Consumer.HouseChanBuffer = 512
	}
	if cfg.Consumer.MaxPendingTriplets <= 0 {
		cfg.Consumer.MaxPendingTriplets = 50000
	}
	if cfg.Consumer.WriteBufferSize <= 0 {
		cfg.Consumer.WriteBufferSize = 256 * 1024
	}
	if cfg.Consumer.StornoFlushEvery <= 0 {
		cfg.Consumer.StornoFlushEvery = 50
	}
	if cfg.Consumer.FetchMinBytes <= 0 {
		cfg.Consumer.FetchMinBytes = 1024
	}
	if cfg.Consumer.FetchMaxBytes <= 0 {
		cfg.Consumer.FetchMaxBytes = 10 * 1024 * 1024
	}
	if cfg.Consumer.OffsetSaveIntervalSec <= 0 {
		cfg.Consumer.OffsetSaveIntervalSec = 30
	}
	return &cfg, nil
}

// stornoStore: house ID based cancellation (full house or specific partition).
// Runs on separate goroutine; main consumer reads via IsStorno / StornoPartitions.
type stornoStore struct {
	mu        sync.RWMutex
	full      map[int]struct{}       // full house storno
	partial   map[int]map[int]struct{} // houseID -> partition numbers to exclude
	path      string
	bootstrap string
	topic     string
	saveMu    sync.Mutex
	saveTimer *time.Timer
}

type stornoMsg struct {
	ID        int `json:"id"`
	Partition int `json:"partition,omitempty"` // 0 = full storno, 1/2/3 = exclude that partition
}

func (s *stornoStore) load() {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}
	var out struct {
		Full    []int            `json:"full"`
		Partial map[int][]int    `json:"partial"`
	}
	if json.Unmarshal(data, &out) != nil {
		return
	}
	s.full = make(map[int]struct{})
	for _, id := range out.Full {
		s.full[id] = struct{}{}
	}
	s.partial = make(map[int]map[int]struct{})
	for id, parts := range out.Partial {
		m := make(map[int]struct{})
		for _, p := range parts {
			m[p] = struct{}{}
		}
		s.partial[id] = m
	}
}

func (s *stornoStore) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	full := make([]int, 0, len(s.full))
	for id := range s.full {
		full = append(full, id)
	}
	partial := make(map[int][]int)
	for id, m := range s.partial {
		parts := make([]int, 0, len(m))
		for p := range m {
			parts = append(parts, p)
		}
		partial[id] = parts
	}
	data, err := json.Marshal(map[string]interface{}{"full": full, "partial": partial})
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	os.MkdirAll(dir, 0755)
	tmpF, err := os.CreateTemp(dir, "storno_*.tmp")
	if err != nil {
		return err
	}
	defer tmpF.Close()
	if _, err := tmpF.Write(data); err != nil {
		os.Remove(tmpF.Name())
		return err
	}
	tmpF.Sync()
	if err := tmpF.Close(); err != nil {
		os.Remove(tmpF.Name())
		return err
	}
	return os.Rename(tmpF.Name(), s.path)
}

func (s *stornoStore) add(id int, partition int) {
	s.mu.Lock()
	if partition == 0 {
		s.full[id] = struct{}{}
		delete(s.partial, id)
	} else {
		if s.partial[id] == nil {
			s.partial[id] = make(map[int]struct{})
		}
		s.partial[id][partition] = struct{}{}
		delete(s.full, id)
	}
	s.mu.Unlock()
	s.scheduleSave()
}

func (s *stornoStore) scheduleSave() {
	const debounceMs = 100
	s.saveMu.Lock()
	if s.saveTimer != nil {
		s.saveTimer.Stop()
	}
	s.saveTimer = time.AfterFunc(debounceMs*time.Millisecond, func() {
		if err := s.save(); err != nil {
			log.Printf("Storno save: %v", err)
		}
		s.saveMu.Lock()
		s.saveTimer = nil
		s.saveMu.Unlock()
	})
	s.saveMu.Unlock()
}

func (s *stornoStore) IsStorno(id int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.full[id]
	return ok
}

func (s *stornoStore) ExcludedPartitions(id int) map[int]struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, full := s.full[id]; full {
		return map[int]struct{}{1: {}, 2: {}, 3: {}}
	}
	if m := s.partial[id]; m != nil {
		cp := make(map[int]struct{}, len(m))
		for k, v := range m {
			cp[k] = v
		}
		return cp
	}
	return nil
}

func (s *stornoStore) runConsumer(ctx context.Context) {
	if s.topic == "" || s.bootstrap == "" {
		return
	}
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	consumer, err := sarama.NewConsumer([]string{s.bootstrap}, config)
	if err != nil {
		log.Printf("Storno consumer: %v (storno disabled)", err)
		return
	}
	defer consumer.Close()
	pc, err := consumer.ConsumePartition(s.topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("Storno partition: %v (storno disabled)", err)
		return
	}
	defer pc.Close()
	log.Printf("Storno goroutine: consuming from %s", s.topic)
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-pc.Messages():
			if !ok {
				return
			}
			var m stornoMsg
			if json.Unmarshal(msg.Value, &m) == nil && m.ID >= 0 {
				s.add(m.ID, m.Partition)
				log.Printf("Storno: id=%d partition=%d", m.ID, m.Partition)
			} else if raw := string(msg.Value); raw != "" {
				if id, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil && id >= 0 {
					s.add(int(id), 0)
					log.Printf("Storno: id=%d (full)", id)
				}
			}
		}
	}
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
		if p, err := strconv.ParseInt(k, 10, 32); err == nil {
			out[int32(p)] = v
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
		raw[strconv.FormatInt(int64(k), 10)] = v
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmpF, err := os.CreateTemp(dir, "consumer_offsets_*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmpF.Name()
	if _, err := tmpF.Write(data); err != nil {
		tmpF.Close()
		os.Remove(tmpPath)
		return err
	}
	_ = tmpF.Sync()
	tmpF.Close()
	if err := os.Rename(tmpPath, s.path); err != nil {
		os.Remove(tmpPath)
		// On Windows, rename over open file may fail; try direct write as fallback
		if err := os.WriteFile(s.path, data, 0644); err != nil {
			return err
		}
	}
	return nil
}

func rewriteHousesRemoveStorno(path string, store *stornoStore) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := SplitLines(data)
	if len(lines) == 0 {
		return nil
	}
	dir := filepath.Dir(path)
	tmpF, err := os.CreateTemp(dir, "houses_*.jsonl.tmp")
	if err != nil {
		return err
	}
	defer tmpF.Close()
	var removed int
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var h HouseJSON
		if json.Unmarshal(line, &h) != nil {
			tmpF.Write(line)
			tmpF.WriteString("\n")
			continue
		}
		if store.IsStorno(h.ID) || len(store.ExcludedPartitions(h.ID)) > 0 {
			removed++
			continue
		}
		tmpF.Write(line)
		tmpF.WriteString("\n")
	}
	tmpF.Sync()
	tmpPath := tmpF.Name()
	tmpF.Close()
	if removed > 0 {
		if err := os.Rename(tmpPath, path); err != nil {
			os.Remove(tmpPath)
			return err
		}
		log.Printf("Removed %d storno'd houses from %s", removed, path)
	} else {
		os.Remove(tmpPath)
	}
	return nil
}

// loadWrittenHouseIDs returns IDs already in houses.jsonl (idempotens feldolgozáshoz)
func loadWrittenHouseIDs(path string) map[int]struct{} {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return nil
	}
	ids := make(map[int]struct{})
	for _, line := range SplitLines(data) {
		if len(line) == 0 {
			continue
		}
		var h HouseJSON
		if json.Unmarshal(line, &h) == nil {
			ids[h.ID] = struct{}{}
		}
	}
	return ids
}

func SplitLines(data []byte) [][]byte {
	var lines [][]byte
	for len(data) > 0 {
		i := 0
		for i < len(data) && data[i] != '\n' {
			i++
		}
		lines = append(lines, data[:i])
		if i < len(data) {
			i++
		}
		data = data[i:]
	}
	return lines
}

func runStornoWorker(store *stornoStore, w *bufio.Writer, f *os.File, houses <-chan HouseJSON, wg *sync.WaitGroup, flushEvery int) {
	defer wg.Done()
	if flushEvery <= 0 {
		flushEvery = 50
	}
	var mu sync.Mutex
	var count int
	for house := range houses {
		var out *StornoHouseJSON
		if store.IsStorno(house.ID) {
			out = &StornoHouseJSON{ID: house.ID, House: house.House, Storno: "full"}
		} else if excluded := store.ExcludedPartitions(house.ID); len(excluded) > 0 {
			filtered := make(map[string]*PartitionOut)
			for k, v := range house.House {
				partNum := 0
				if strings.HasPrefix(k, "partition_") {
					partNum, _ = strconv.Atoi(k[10:])
				}
				if _, ex := excluded[partNum]; !ex {
					filtered[k] = v
				}
			}
			excl := make([]int, 0, len(excluded))
			for p := range excluded {
				excl = append(excl, p)
			}
			out = &StornoHouseJSON{ID: house.ID, House: filtered, Storno: "partial", ExcludedPartitions: excl}
		}
		if out != nil {
			jsonBytes, _ := json.Marshal(out)
			mu.Lock()
			w.Write(append(jsonBytes, '\n'))
			count++
			if count%flushEvery == 0 && f != nil {
				w.Flush()
				f.Sync()
			}
			mu.Unlock()
		}
	}
}

func runSimpleConsumer(cfg *Config, f *os.File, w *bufio.Writer, stornoF *os.File, stornoW *bufio.Writer, sigChan <-chan os.Signal, storno *stornoStore, stopStorno context.CancelFunc) {
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

	// Időzített offset mentés (pl. 30 mp) – csökkenti az adatvesztést crash esetén
	if intervalSec := cfg.Consumer.OffsetSaveIntervalSec; intervalSec > 0 {
		go func() {
			ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				if err := offsetStore.save(); err != nil {
					log.Printf("Offset periodic save error: %v", err)
				}
			}
		}()
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0
	saramaConfig.Consumer.Fetch.Min = int32(cfg.Consumer.FetchMinBytes)
	saramaConfig.Consumer.Fetch.Max = int32(cfg.Consumer.FetchMaxBytes)
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

	var houseChan chan HouseJSON
	var stornoWg sync.WaitGroup
	var consumerWg sync.WaitGroup
	chanBuf := cfg.Consumer.HouseChanBuffer
	if chanBuf <= 0 {
		chanBuf = 512
	}
	if storno != nil && stornoW != nil {
		houseChan = make(chan HouseJSON, chanBuf)
		stornoWg.Add(1)
		go runStornoWorker(storno, stornoW, stornoF, houseChan, &stornoWg, cfg.Consumer.StornoFlushEvery)
	}
	maxPending := cfg.Consumer.MaxPendingTriplets
	if maxPending <= 0 {
		maxPending = 50000
	}

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
		consumerWg.Add(1)

		go func(part int32) {
			defer consumerWg.Done()
			for msg := range pc.Messages() {
				raw := string(msg.Value)
				parsed, err := parseMsg(raw)
				if err != nil {
					continue
				}
				mu.Lock()
				if parts[parsed.ID] == nil {
					if len(parts) >= maxPending {
						evictID := 0
						for id := range parts {
							if evictID == 0 || id < evictID {
								evictID = id
							}
						}
						delete(parts, evictID)
						log.Printf("Evicted pending triplet id=%d (max_pending=%d)", evictID, maxPending)
					}
					parts[parsed.ID] = make(map[int]*partWithOffset)
				}
				parts[parsed.ID][parsed.Part] = &partWithOffset{parsed: parsed, offset: msg.Offset}
				if len(parts[parsed.ID]) == 3 {
					e1, e2, e3 := parts[parsed.ID][1], parts[parsed.ID][2], parts[parsed.ID][3]
					houseMap := map[string]*PartitionOut{
						"partition_1": {Ts: e1.parsed.Ts, Name: e1.parsed.Name, Task: e1.parsed.Task, Offset: e1.offset},
						"partition_2": {Ts: e2.parsed.Ts, Name: e2.parsed.Name, Task: e2.parsed.Task, Offset: e2.offset},
						"partition_3": {Ts: e3.parsed.Ts, Name: e3.parsed.Name, Task: e3.parsed.Task, Offset: e3.offset},
					}
					house := HouseJSON{ID: parsed.ID, House: houseMap}
					hasStorno := storno != nil && (storno.IsStorno(parsed.ID) || len(storno.ExcludedPartitions(parsed.ID)) > 0)
					if !hasStorno {
						jsonBytes, _ := json.Marshal(house)
						if _, err := w.Write(append(jsonBytes, '\n')); err != nil {
							log.Printf("Write error: %v", err)
							mu.Unlock()
							continue
						}
						w.Flush()
						f.Sync()
					}
					if stornoW != nil && hasStorno {
						select {
						case houseChan <- house:
						default:
						}
					}
					offsetStore.update(0, e1.offset)
					offsetStore.update(1, e2.offset)
					offsetStore.update(2, e3.offset)
					cnt := atomic.AddInt64(&tripletCount, 1)
					flushEvery := int64(cfg.Consumer.FlushEvery)
					if flushEvery <= 0 {
						flushEvery = 100
					}
					if cnt%flushEvery == 0 {
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
	stornoOut := "none"
	if stornoW != nil {
		stornoOut = cfg.Consumer.StornoOutputFile
	}
	periodicSave := ""
	if cfg.Consumer.OffsetSaveIntervalSec > 0 {
		periodicSave = fmt.Sprintf(" | Offset save every %ds", cfg.Consumer.OffsetSaveIntervalSec)
	}
	fmt.Printf("Output: %s | Storno: %s | Offsets: %s | Flush every %d | Log every %d%s\n", cfg.Consumer.OutputFile, stornoOut, offsetPath, cfg.Consumer.FlushEvery, cfg.Consumer.LogEvery, periodicSave)
	fmt.Println("---")

	<-sigChan
	if stopStorno != nil {
		stopStorno()
	}
	log.Println("Shutdown signal received, flushing and saving offsets...")
	go func() {
		time.Sleep(5 * time.Second)
		log.Println("Forcing exit...")
		os.Exit(0)
	}()
	consumer.Close()
	consumerWg.Wait()
	if houseChan != nil {
		close(houseChan)
		stornoWg.Wait()
	}
	w.Flush()
	f.Sync()
	if stornoW != nil {
		stornoW.Flush()
		if stornoF != nil {
			stornoF.Sync()
		}
	}
	if err := offsetStore.save(); err != nil {
		log.Printf("Offset save on shutdown: %v", err)
	}
}

type tripletHandler struct {
	cfg          *Config
	w            *bufio.Writer
	f            *os.File
	parts        map[int]map[int]*partEntry
	writtenIDs   map[int]struct{} // idempotens: már kiírt ID-k
	mu           sync.Mutex
	tripletCount int64
	maxPending   int
	ready        chan struct{}
	readyOnce    sync.Once
}

func (h *tripletHandler) Setup(session sarama.ConsumerGroupSession) error {
	claims := session.Claims()
	for topic, parts := range claims {
		log.Printf("ConsumerGroup Setup: topic=%s assigned_partitions=%v (total=%d)", topic, parts, len(parts))
	}
	h.readyOnce.Do(func() { close(h.ready) })
	return nil
}

func (h *tripletHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *tripletHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("ConsumeClaim: partition %d started", claim.Partition())
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
			if len(h.parts) >= h.maxPending {
				evictID := 0
				for id := range h.parts {
					if evictID == 0 || id < evictID {
						evictID = id
					}
				}
				delete(h.parts, evictID)
				log.Printf("Evicted pending triplet id=%d (max_pending=%d)", evictID, h.maxPending)
			}
			h.parts[parsed.ID] = make(map[int]*partEntry)
		}
		h.parts[parsed.ID][parsed.Part] = &partEntry{parsed: parsed, msg: msg}
		if len(h.parts[parsed.ID]) == 3 {
			e1, e2, e3 := h.parts[parsed.ID][1], h.parts[parsed.ID][2], h.parts[parsed.ID][3]
			house := HouseJSON{
				ID: parsed.ID,
				House: map[string]*PartitionOut{
					"partition_1": {Ts: e1.parsed.Ts, Name: e1.parsed.Name, Task: e1.parsed.Task, Offset: e1.msg.Offset},
					"partition_2": {Ts: e2.parsed.Ts, Name: e2.parsed.Name, Task: e2.parsed.Task, Offset: e2.msg.Offset},
					"partition_3": {Ts: e3.parsed.Ts, Name: e3.parsed.Name, Task: e3.parsed.Task, Offset: e3.msg.Offset},
				},
			}
			if _, already := h.writtenIDs[parsed.ID]; already {
				session.MarkMessage(e1.msg, "")
				session.MarkMessage(e2.msg, "")
				session.MarkMessage(e3.msg, "")
				delete(h.parts, parsed.ID)
				h.mu.Unlock()
				continue
			}
			jsonBytes, _ := json.Marshal(house)
			if _, err := h.w.Write(append(jsonBytes, '\n')); err != nil {
				log.Printf("Write error: %v", err)
				h.mu.Unlock()
				continue
			}
			h.writtenIDs[parsed.ID] = struct{}{}
			cnt := atomic.AddInt64(&h.tripletCount, 1)
			if cnt == 1 {
				log.Printf("First triplet received (partition %d)", claim.Partition())
			}
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
	saramaConfig.Consumer.Fetch.Min = int32(cfg.Consumer.FetchMinBytes)
	saramaConfig.Consumer.Fetch.Max = int32(cfg.Consumer.FetchMaxBytes)
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	// Coordinator stabilitás: hosszabb session + heartbeat
	saramaConfig.Consumer.Group.Session.Timeout = 60 * time.Second
	saramaConfig.Consumer.Group.Heartbeat.Interval = 10 * time.Second
	saramaConfig.Consumer.Group.Rebalance.Timeout = 60 * time.Second
	// Friss metadata a partition assignment előtt (default 10min lehet elavult)
	saramaConfig.Metadata.RefreshFrequency = 5 * time.Second

	maxPending := cfg.Consumer.MaxPendingTriplets
	if maxPending <= 0 {
		maxPending = 50000
	}
	writtenIDs := loadWrittenHouseIDs(cfg.Consumer.OutputFile)
	if writtenIDs == nil {
		writtenIDs = make(map[int]struct{})
	} else {
		log.Printf("Idempotent: loaded %d existing house IDs from output", len(writtenIDs))
	}
	handler := &tripletHandler{
		cfg:        cfg,
		w:          w,
		f:          f,
		parts:      make(map[int]map[int]*partEntry),
		writtenIDs: writtenIDs,
		maxPending: maxPending,
		ready:      make(chan struct{}),
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
		// Force exit after 5s if graceful shutdown hangs (e.g. Kafka leave-group timeout)
		go func() {
			time.Sleep(5 * time.Second)
			log.Println("Forcing exit...")
			os.Exit(0)
		}()
	}()

	fmt.Printf("Consumer (group) connected. Topic: %s | Group: %s | Offsets stored by Kafka | Idempotent\n", topicName, groupID)
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
	log.Println("=== PROCESS: Consumer (Go) | houses -> output ===")
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Config: %v", err)
	}

	outputDir := filepath.Dir(cfg.Consumer.OutputFile)
	os.MkdirAll(outputDir, 0755)
	f, err := os.OpenFile(cfg.Consumer.OutputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Output file: %v", err)
	}
	defer func() {
		f.Sync()
		f.Close()
	}()

	bufSize := cfg.Consumer.WriteBufferSize
	if bufSize < 4096 {
		bufSize = 4096
	}
	w := bufio.NewWriterSize(f, bufSize)
	defer w.Flush()

	var stornoF *os.File
	var stornoW *bufio.Writer
	if cfg.Consumer.StornoOutputFile != "" {
		sf, err := os.OpenFile(cfg.Consumer.StornoOutputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Storno output file: %v (storno houses will not be written)", err)
		} else {
			stornoF = sf
			stornoBuf := cfg.Consumer.WriteBufferSize
			if stornoBuf < 4096 {
				stornoBuf = 4096
			}
			stornoW = bufio.NewWriterSize(sf, stornoBuf)
			defer func() {
				stornoW.Flush()
				stornoF.Sync()
				stornoF.Close()
			}()
		}
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	bootstrap := cfg.Kafka.BootstrapServers
	if bootstrap == "" {
		bootstrap = broker
	}
	stornoPath := cfg.Consumer.StornoFile
	if stornoPath == "" {
		stornoPath = filepath.Join(filepath.Dir(cfg.Consumer.OutputFile), "storno_ids.json")
	}
	stornoStore := &stornoStore{
		full:      make(map[int]struct{}),
		partial:   make(map[int]map[int]struct{}),
		path:      stornoPath,
		bootstrap: bootstrap,
		topic:     cfg.Consumer.StornoTopic,
	}
	stornoStore.load()

	housesPath := cfg.Consumer.OutputFile
	if path, err := filepath.Abs(housesPath); err == nil {
		housesPath = path
	}
	if _, err := os.Stat(housesPath); err == nil {
		if rewriteHousesRemoveStorno(housesPath, stornoStore) != nil {
			log.Printf("Warning: could not rewrite houses to remove storno'd IDs")
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go stornoStore.runConsumer(ctx)

	if cfg.Consumer.ConsumerGroup {
		runConsumerGroup(cfg, f, w, sigChan)
	} else {
		runSimpleConsumer(cfg, f, w, stornoF, stornoW, sigChan, stornoStore, cancel)
	}
}
