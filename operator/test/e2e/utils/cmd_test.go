package utils

import (
	"strings"
	"testing"
)

func TestExecCmdWithOutput(t *testing.T) {
	type args struct {
		cmd  string
		args []string
	}
	tests := []struct {
		name      string
		args      args
		stdoutHas string
		stderrHas string
		wantErr   bool
	}{
		{
			name: "test no command",
			args: args{
				cmd:  "no_this_command",
				args: []string{},
			},
			stdoutHas: "",
			stderrHas: "",
			wantErr:   true,
		},
		{
			name: "test ls",
			args: args{
				cmd: "ls",
				args: []string{
					"-l",
					"/",
				},
			},
			stdoutHas: "usr",
			stderrHas: "",
			wantErr:   false,
		},
		{
			name: "test ls /no_dir",
			args: args{
				cmd: "ls",
				args: []string{
					"-l",
					"/no_dir",
				},
			},
			stdoutHas: "",
			stderrHas: "No such file or directory",
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdout, stderr, err := ExecCmdWithOutput(tt.args.cmd, tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecCmdWithOutput() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !strings.Contains(stdout, tt.stdoutHas) {
				t.Errorf("ExecCmdWithOutput() stdout = %v, stdoutHas %v", stdout, tt.stdoutHas)
			}
			if !strings.Contains(stderr, tt.stderrHas) {
				t.Errorf("ExecCmdWithOutput() stderr = %v, stdoutHas %v", stderr, tt.stderrHas)
			}
		})
	}
}
