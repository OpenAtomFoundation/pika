# README
这是用golang编写的pika 集成测试代码,默认提交代码到pika仓库后会自动运行。

## 本地跑golang集成测试
如果你想在本地运行测试，需要完成以下的准备工作：

### 1.准备程序和配置文件
在../../output/pika目录确保有编译好的pika程序。  
（也可以提前编译好mac版本的pika程序,并手动将pika文件拷贝到start_master_and_slave.sh中制定的目录，将pika未改动的conf文件拷贝到test目录；或者直接修改start_master_and_slave.sh启动路径。）

手动执行测试的前提是，已安装ginkgo,例如
```
cd tests/integration/
go get github.com/onsi/ginkgo/v2/ginkgo
go install github.com/onsi/ginkgo/v2/ginkgo
go get github.com/onsi/gomega/...
```

### 2.启动Pika服务
在项目主目录下执行  
```
cd tests  

sh ./integration/start_master_and_slave.sh
```

### 3.运行测试
在tests目录下执行
cd integration
sh integrate_test.sh

### 4.运行指定文件的测试


添加环境变量  
```
go env |grep GOBIN  
export PATH="$PATH:$GOBIN"
```

执行`ginkgo --focus-file="slowlog_test.go" -vv`

ginkgo框架参考: https://onsi.github.io/ginkgo/#mental-model-ginkgo-assumes-specs-are-independent  
备注:  
`--focus-file`执行匹配文件  
`--skip-file`过滤不匹配的文件  
`--focus`执行匹配描述的测试  
`--skip`过滤匹配描述的测试  
例如，`ginkgo --focus=dog --focus=fish --skip=cat --skip=purple`
则只运行运行It(描述内容中)例如"likes dogs"、"likes dog fish"的单测，而跳过"purple"相关的测试。
