package utils

import (
	"io"
	"os/exec"
	"sync"
)

// ExecCmdWithOutput executes a command and returns the output.
// return stdoutHas,stderrHas,err
func ExecCmdWithOutput(cmd string, args ...string) (string, string, error) {
	cmdSt := exec.Command(cmd, args...)
	stdoutReader, err := cmdSt.StdoutPipe()
	if err != nil {
		panic(err)
	}
	stderrReader, err := cmdSt.StderrPipe()
	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(2)

	var stdout, stderr []byte
	go func() {
		defer wg.Done()
		stdout, err = io.ReadAll(stdoutReader)
		if err != nil {
			return
		}
	}()
	go func() {
		defer wg.Done()
		stderr, err = io.ReadAll(stderrReader)
		if err != nil {
			return
		}
	}()

	err = cmdSt.Run()
	wg.Wait()
	return string(stdout), string(stderr), err
}
