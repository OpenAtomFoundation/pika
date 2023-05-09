package utils

import (
	"fmt"
	"os"
	"path"
	"time"
)

// StoreTmpFile stores the content to a temporary file and returns the file path.
func StoreTmpFile(content string) (string, error) {
	tmpFileName := fmt.Sprintf("pika-test-tmp-file-%d", time.Now().UnixNano())
	tmpDir := os.TempDir()
	tmpFile := path.Join(tmpDir, tmpFileName)
	f, err := os.Create(tmpFile)
	if err != nil {
		return "", err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {

		}
	}(f)

	_, err = f.WriteString(content)
	if err != nil {
		return "", err
	}
	return tmpFile, nil
}
