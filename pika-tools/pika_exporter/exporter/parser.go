package exporter

import (
	"bufio"
	"errors"
	"strings"

	"github.com/Masterminds/semver"
)

const (
	keyValueSeparator   = ":"
	doubleQuotationMark = "\""
	pikaVersionKey      = "pika_version"
)

// 解析info命令信息，返回version信息与其他指标map[string]string，异常
func parseInfo(info string) (*semver.Version, map[string]string, error) {
	extracts, err := extractInfo(info)
	if err != nil {
		return nil, nil, err
	}

	version, err := semver.NewVersion(getVersion(extracts))
	if err != nil {
		return nil, nil, errors.New("invalid version in info")
	}

	return version, extracts, nil
}

// 将返回的信息解析为map
func extractInfo(s string) (map[string]string, error) {
	m := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(s))
	for scanner.Scan() {
		line := scanner.Text()

		cleanLine := trimSpace(line)
		if cleanLine == "" {
			continue
		}

		k, v := fetchKV(cleanLine)
		m[k] = v
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return m, scanner.Err()
}

func trimSpace(s string) string {
	s = strings.TrimLeft(s, " ")
	s = strings.TrimRight(s, " ")
	return s
}

// 根据keyValueSeparator分割，返回kv或者k,nil
func fetchKV(s string) (k, v string) {
	//strings.Index返回匹配到的位置：Index returns the index of the first instance of substr in s, or -1 if substr is not present in s.
	pos := strings.Index(s, keyValueSeparator)
	if pos < 0 {
		k = s
		return
	}

	k = trimSpace(s[:pos])
	v = trimSpace(s[pos+1:])
	return
}

func getVersion(extracted map[string]string) (version string) {
	version, _ = extracted[pikaVersionKey]
	vv := strings.Split(version, ".")
	if len(vv) > 3 {
		version = strings.Join(vv[:3], ".")
	}
	return
}
