package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	flagLine_allStats                   = "ALL STATS"
	flagLine_requestLatencyDistribution = "Request Latency Distribution"
	flagLine_dividingLineBeginPrefix    = "----"
	flagWord_totals                     = "Totals"
	flagWord_end                        = "WAIT"
)

var (
	inPath  string
	outPath string
)

func main() {
	flag.StringVar(&inPath, "in_dir", "", "benchmark result file path to parse")
	flag.StringVar(&outPath, "out_dir", "", "parsed result file path to save")
	flag.Parse()

	if inPath == "" || outPath == "" {
		log.Fatalf("in_dir and out_dir should not be empty")
	}

	parseRWData()
	parseCmdData()
}

func parseRWData() {
	dirs, err := os.ReadDir(inPath)
	if err != nil {
		log.Fatal(err)
	}

	benchFiles := map[string]string{}

	for _, dir := range dirs {
		if !dir.IsDir() && strings.HasPrefix(dir.Name(), "rw_") {
			key := strings.TrimPrefix(dir.Name(), "rw_")
			key = strings.TrimSuffix(key, ".txt")
			key = strings.TrimSpace(key)
			benchFiles[key] = inPath + "/" + dir.Name()
		}
	}

	opsOutPathPrefix := outPath + "/rw_ops"
	lantencyOutPathPrefix := outPath + "/rw_lantency"
	if err = os.MkdirAll(opsOutPathPrefix, os.ModePerm); err != nil {
		log.Fatalf("Error creating directory %s: %v", opsOutPathPrefix, err)
	}
	if err = os.MkdirAll(lantencyOutPathPrefix, os.ModePerm); err != nil {
		log.Fatalf("Error creating directory %s: %v", lantencyOutPathPrefix, err)
	}

	for name, fileName := range benchFiles {
		data := DoParse(name, fileName, true)
		writeBenchData(opsOutPathPrefix, lantencyOutPathPrefix, data)
	}
}

func parseCmdData() {
	dirs, err := os.ReadDir(inPath)
	if err != nil {
		log.Fatal(err)
	}

	benchFiles := map[string]string{}

	for _, dir := range dirs {
		if !dir.IsDir() && strings.HasPrefix(dir.Name(), "cmd_") {
			key := strings.TrimPrefix(dir.Name(), "cmd_")
			key = strings.TrimSuffix(key, ".txt")
			key = strings.TrimSpace(key)
			benchFiles[key] = inPath + "/" + dir.Name()
		}
	}

	opsOutPathPrefix := outPath + "/cmd_ops"
	lantencyOutPathPrefix := outPath + "/cmd_lantency"
	if err = os.MkdirAll(opsOutPathPrefix, os.ModePerm); err != nil {
		log.Fatalf("Error creating directory %s: %v", opsOutPathPrefix, err)
	}
	if err = os.MkdirAll(lantencyOutPathPrefix, os.ModePerm); err != nil {
		log.Fatalf("Error creating directory %s: %v", lantencyOutPathPrefix, err)
	}

	for name, fileName := range benchFiles {
		data := DoParse(name, fileName, false)
		writeBenchData(opsOutPathPrefix, lantencyOutPathPrefix, data)
	}
}

func writeBenchData(opsPath, lantencyPath string, data BenchData) {
	parsedLantencyDatas := data.GetParedLantencyData()
	for i := range parsedLantencyDatas {
		writeLantencyFile(lantencyPath, parsedLantencyDatas[i])
	}
	parsedOpsData := data.GetParedOpsData()
	writeOpsFile(opsPath, parsedOpsData)
}

func writeLantencyFile(path string, data ParsedLatencyData) {
	fileName := path + "/" + data.Title
	personJSON, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Fatal("Error marshalling JSON:", err)
		return
	}
	doWriteFile(fileName, personJSON)
}

func writeOpsFile(path string, data ParsedOpsData) {
	fileName := path + "/" + data.Title
	personJSON, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Fatal("Error marshalling JSON:", err)
		return
	}
	doWriteFile(fileName, personJSON)
}

func doWriteFile(fileName string, jsonData []byte) {
	var (
		file *os.File
		err  error
	)

	if _, err = os.Stat(fileName); err == nil {
		if err = os.Remove(fileName); err != nil {
			log.Fatal("Error removing file:", err)
		}
	}
	if file, err = os.Create(fileName); err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	if _, err = file.Write(jsonData); err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}
}

type BenchData struct {
	usePrefix         bool                          `json:"-"`
	Title             string                        `json:"title"`
	OpsPerSes         float64                       `json:"opsPerSes"`
	CommandLatencyMap map[string]map[string]float64 `json:"commandLatencyMap"`
}

type ParsedLatencyData struct {
	Title      string             `json:"title"`
	LatencyMap map[string]float64 `json:"latencyMap"`
}

type ParsedOpsData struct {
	Title     string  `json:"title"`
	OpsPerSes float64 `json:"opsPerSes"`
}

func newBenchResult(title string, usePrefix bool) BenchData {
	return BenchData{
		Title:             title,
		usePrefix:         usePrefix,
		CommandLatencyMap: make(map[string]map[string]float64),
	}
}

func (b *BenchData) GetParedOpsData() ParsedOpsData {
	return ParsedOpsData{
		Title:     strings.ToLower(b.Title),
		OpsPerSes: b.OpsPerSes,
	}
}

func (b *BenchData) GetParedLantencyData() []ParsedLatencyData {
	parsedLantencyDatas := make([]ParsedLatencyData, 0)
	for s, m := range b.CommandLatencyMap {
		title := s
		if b.usePrefix {
			title = b.Title + "-" + s
		}
		parsedBenchData := ParsedLatencyData{
			Title:      strings.ToLower(title),
			LatencyMap: m,
		}
		parsedLantencyDatas = append(parsedLantencyDatas, parsedBenchData)
	}
	return parsedLantencyDatas
}

func DoParse(name, filePath string, usePrefix bool) BenchData {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	var (
		hasAllStatsLine                   = false
		hasRequestLatencyDistributionLine = false
		hasDividingLineBegin              = false
		res                               = newBenchResult(name, usePrefix)
	)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		if line == flagLine_allStats {
			hasAllStatsLine = true
			continue
		} else if line == flagLine_requestLatencyDistribution {
			hasRequestLatencyDistributionLine = true
			continue
		} else if hasRequestLatencyDistributionLine {
			if strings.HasPrefix(line, flagLine_dividingLineBeginPrefix) {
				hasDividingLineBegin = true
				continue
			} else if strings.HasPrefix(line, flagWord_end) {
				break
			}
		}

		if hasAllStatsLine && strings.HasPrefix(line, flagWord_totals) {
			values := strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, flagWord_totals)))
			if len(values) == 0 {
				log.Fatalf("Totals line shouldn't be null")
			}

			opsRowValue := values[0]
			opsPerSes, err := strconv.ParseFloat(opsRowValue, 64)
			if err != nil {
				log.Fatalf("Error parsing OpsPerSes: %v", err)
			}
			res.OpsPerSes = opsPerSes

			hasAllStatsLine = false
			continue
		}

		if hasDividingLineBegin {
			values := strings.Fields(strings.TrimSpace(line))
			if len(values) != 3 {
				continue
			}

			var (
				msec    float64
				percent float64
			)

			if msec, err = strconv.ParseFloat(values[1], 64); err != nil {
				log.Fatalf("Error parsing msec: %s", msec)
			}
			if percent, err = strconv.ParseFloat(values[2], 64); err != nil {
				log.Fatalf("Error parsing percent: %s", percent)
			}

			if _, exists := res.CommandLatencyMap[values[0]]; !exists {
				res.CommandLatencyMap[values[0]] = make(map[string]float64)
			}
			res.CommandLatencyMap[values[0]][fmt.Sprintf("%.3f", percent)] = msec
		}

	}

	if err = scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
	return res
}
