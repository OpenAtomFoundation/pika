# README
This is an integration test code for Pika written in Golang. By default, the tests are automatically executed after code is submitted to the Pika repository.

[中文](https://github.com/OpenAtomFoundation/pika/blob/unstable/tests/integration/README_CN.md)

## Running Golang Integration Tests Locally
If you want to run the tests locally, you need to complete the following preparations:

### 1. Prepare the Program and Configuration Files
Ensure that the compiled Pika program is present in the ../../output/pika directory.
(You can also compile the Pika program for Mac in advance and manually copy the Pika file to the directory specified in start_master_and_slave.sh, copy the unchanged pika configuration files to the test directory; or directly modify the startup path in start_master_and_slave.sh.)

The prerequisite for manually executing the tests is having Ginkgo installed, for example:
```
cd tests/integration/
go get github.com/onsi/ginkgo/v2/ginkgo
go install github.com/onsi/ginkgo/v2/ginkgo
go get github.com/onsi/gomega/...
```

### 2.Start the Pika Service
Execute in the project root directory:
```
cd tests  

sh ./integration/start_master_and_slave.sh
```

### 3.Run Tests
Execute in the tests directory:
```
cd integration  
sh integrate_test.sh
```

### 4.Run Tests for a Specific File

Add environment variables:
```
go env |grep GOBIN  
export PATH="$PATH:$GOBIN"
```

Execute`ginkgo --focus-file="slowlog_test.go" -vv`

Refer to the Ginkgo framework: https://onsi.github.io/ginkgo/#mental-model-ginkgo-assumes-specs-are-independent  
Note:  
`--focus-file` executes matching files

`--skip-file` filters out non-matching files

`--focus` executes tests matching descriptions

`--skip` filters out tests matching descriptions

For example, `ginkgo --focus=dog --focus=fish --skip=cat --skip=purple`

This will only run tests described as "likes dogs", "likes dog fish", while skipping tests related to "purple".
