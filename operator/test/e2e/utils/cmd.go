package utils

import (
	"io"
	"os/exec"
)

// ExecCmdWithOutput executes a command and returns the output.
// return stdoutHas,stderrHas,err
func ExecCmdWithOutput(cmd string, args ...string) (string, string, error) {
	cmdSt := exec.Command(cmd, args...)

	stdoutReader, err := cmdSt.StdoutPipe()
	if err != nil {
		return "", "", err
	}
	stderrReader, err := cmdSt.StderrPipe()
	if err != nil {
		return "", "", err
	}

	err = cmdSt.Start()
	if err != nil {
		return "", "", err
	}

	stdout, err := io.ReadAll(stdoutReader)
	if err != nil {
		return "", "", err
	}

	stderr, err := io.ReadAll(stderrReader)
	if err != nil {
		return "", "", err
	}

	err = cmdSt.Wait()
	return string(stdout), string(stderr), err
}
