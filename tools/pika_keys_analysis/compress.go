package pika_keys_analysis

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"os"
	"path/filepath"
)

func compress(data []byte) ([]byte, error) {
	var compressedData bytes.Buffer
	writer := gzip.NewWriter(&compressedData)

	_, err := writer.Write(data)
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	return compressedData.Bytes(), nil
}

func decompress(compressedData []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, err
	}

	decompressedData, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	err = reader.Close()
	if err != nil {
		return nil, err
	}

	return decompressedData, nil
}

func isCompressed(data []byte) bool {
	return len(data) > 2 && data[0] == 0x1f && data[1] == 0x8b
}

// saveLocal saves the key-value pair to local file system.
func saveLocal(key []byte, value []byte) error {
	_, err := os.ReadDir(Save)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(Save, 0755)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	filename := filepath.Join(Save, string(key))
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(value)
	return err
}
