# ifeq "$(GOPATH)" ""
#   $(error Please set the environment variable GOPATH before running `make`)
# endif

# export PATH := $(PATH):$(GOPATH)/bin

OS := $(shell uname)
ARCH := $(shell uname -m)
# for mac
BRANCH			:= $(shell git branch | sed  's/* \(.*\)/\1/p')
# for Linux
# BRANCH			:= $(shell git branch | sed --quiet 's/* \(.*\)/\1/p')
GITREV 			:= $(shell git rev-parse --short HEAD)
BUILDTIME 		:= $(shell date '+%F %T %Z')
COMPILERVERSION	:= $(subst go version ,,$(shell go version))
PROJNAME        := pika_exporter

define GENERATE_VERSION_CODE
cat << EOF | gofmt > version.go
package main

const (
	BuildVersion   = "$(BRANCH)"
    BuildCommitSha = "$(GITREV)"
    BuildDate      = "$(BUILDTIME)"
    GoVersion      = "$(COMPILERVERSION)"
)
EOF
endef
export GENERATE_VERSION_CODE

PACKAGES := $$(go list ./...| grep -vE 'vendor')
FILES    := $$(find . -name '*.go' | grep -vE 'vendor')

define TEST_COVER
#!/bin/bash

set -e

which gocov >/dev/null || go get -v -u github.com/axw/gocov/gocov

COV_FILE=coverage.txt
COV_TMP_FILE=coverage_tmp.txt

rm -f $$COV_FILE
rm -f $$COV_TMP_FILE
touch $$COV_TMP_FILE

echo "mode: count" > $$COV_FILE

for pkg in $(PACKAGES); do
	go test -v $$pkg -covermode=count -coverprofile=$$COV_TMP_FILE
	tail -n +2 $$COV_TMP_FILE >> $$COV_FILE || (echo "Unable to append coverage for $$pkg" && exit 1)
done

gocov convert $$COV_FILE | gocov report | grep 'Total Coverage'

rm -f $$COV_FILE
rm -f $$COV_TMP_FILE

endef
export TEST_COVER

all: build 

build: deps
ifeq ($(OS), Linux)
ifeq ($(ARCH), x86_64)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/$(PROJNAME)
else ifeq ($(ARCH), arm64)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/$(PROJNAME)
endif
else ifeq ($(OS), Darwin)
ifeq ($(ARCH), x86_64)
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o bin/$(PROJNAME)
else ifeq ($(ARCH), arm64)
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o bin/$(PROJNAME)
endif
endif

deps: generateVer
	@mkdir -p bin

generateVer:
	@echo "$$GENERATE_VERSION_CODE" | bash

test:
	@echo "$$TEST_COVER" | bash

race:
	go test -v -race $(PACKAGES)

check:
	which golint >/dev/null || go get -v -u github.com/golang/lint/golint
	@echo "vet"
	@go tool vet $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "vet --shadow"
	@go tool vet --shadow $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "golint"
ifdef LINTEXCEPTION
	@golint $(PACKAGES) | grep -vE'$(LINTEXCEPTION)' | awk '{print} END{if(NR>0) {exit 1}}'
else
	@golint $(PACKAGES) | awk '{print} END{if(NR>0) {exit 1}}'
endif

errcheck:
	which errcheck >/dev/null || go get -v -u github.com/kisielk/errcheck
	errcheck -blank $(PACKAGES)

clean:
	@rm -rf bin
	@go clean -i ./...

dev: check test race build

update:
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
	rm -r vendor
ifdef PKG
	glide get -v --skip-test $(PKG)
else
	glide update -v -u --skip-test
endif
	@echo "removing test files"
	glide vc --only-code --no-tests --use-lock-file