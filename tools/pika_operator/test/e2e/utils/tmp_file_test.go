package utils

import (
	"strings"
	"testing"
)

func TestStoreTmpFile(t *testing.T) {
	type args struct {
		content string
	}
	tests := []struct {
		name    string
		args    args
		wantHas string
		wantErr bool
	}{
		{
			name: "test store tmp file",
			args: args{
				content: "test",
			},
			wantHas: "pika-test-tmp-file-",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := StoreTmpFile(tt.args.content)
			if (err != nil) != tt.wantErr {
				t.Errorf("StoreTmpFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			t.Log(got)
			if !strings.Contains(got, tt.wantHas) {
				t.Errorf("StoreTmpFile() got = %v, wantHas %v", got, tt.wantHas)
			}
		})
	}
}
