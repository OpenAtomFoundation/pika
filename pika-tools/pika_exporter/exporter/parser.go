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

func fetchKV(s string) (k, v string) {
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
