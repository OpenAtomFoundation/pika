package scaffold

import (
	"fmt"
	"github.com/OpenAtomFoundation/pika/operator/test/e2e/utils"
)

// kubectl command related functions.

// GetKubeConfig returns the kubeConfig path.
func (s *Scaffold) GetKubeConfig() string {
	return s.kubeConfig
}

// CreateResourceFromString creates a resource from a string.
func (s *Scaffold) CreateResourceFromString(yamlStr string) error {
	tmpFile, err := utils.StoreTmpFile(yamlStr)
	if err != nil {
		return err
	}
	return s.KubeApply(tmpFile)
}

// KubeApply applies a yaml file.
func (s *Scaffold) KubeApply(filePath string) error {
	stdout, stderr, err := utils.ExecCmdWithOutput("kubectl", "--kubeconfig", s.kubeConfig, "apply", "-f", filePath, "-n", s.namespace)
	if err != nil {
		return fmt.Errorf("failed to apply yaml file %s, stdout: \n%s\n, stderr: \n%s\n, err: %v", filePath, stdout, stderr, err)
	}
	return nil
}

// KubeDelete deletes a yaml file.
func (s *Scaffold) KubeDelete(filePath string) error {
	stdout, stderr, err := utils.ExecCmdWithOutput("kubectl", "delete", "-f", filePath, "-n", s.namespace)
	if err != nil {
		return fmt.Errorf("failed to delete yaml file %s, stdout: \n%s\n, stderr: \n%s\n, err: %v", filePath, stdout, stderr, err)
	}
	return nil
}
