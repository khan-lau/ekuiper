.PHONY: build_prepare all build clean run

BUILD_PATH=dist
KUIPER_SOURCE=
VERSION=


ifeq ($(OS),Windows_NT) 
    uname_S=Windows
	KUIPER_SOURCE=$(shell powershell -Command "(get-location).path")
	VERSION=$(shell powershell "(Invoke-Command -ScriptBlock {git describe --tags --always}).replace('^v', '')")
else ifeq ($(OS),windows) 
    uname_S=Windows
	KUIPER_SOURCE=$(shell powershell -Command "(get-location).path")
	VERSION=$(shell powershell "(Invoke-Command -ScriptBlock {git describe --tags --always}).replace('^v', '')")
else
    uname_S=$(shell uname -s)
	KUIPER_SOURCE=$(shell pwd)
	VERSION=$(shell git describe --tags --always | sed 's/^v//g')
endif

PACKAGE_NAME=kuiper-$(VERSION)-$(uname_S)

#info warning error 
$(info  os: ${uname_S}, ori: $(OS))
$(info  KUIPER_SOURCE: ${KUIPER_SOURCE},PACKAGE_NAME: ${PACKAGE_NAME}, version: ${VERSION})


all: build_prepare build

build:
ifeq ($(uname_S), Windows)
	@cmd /C 'set GO111MODULE=on&&set CGO_ENABLED=0&&set GOOS=windows&&set GOARCH=amd64&&go build -trimpath -ldflags='-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative' -o $(BUILD_PATH)/$(PACKAGE_NAME)/bin/kuiper.exe cmd/kuiper/main.go'
	@cmd /C 'set GO111MODULE=on&&set CGO_ENABLED=1&&set GOOS=windows&&set GOARCH=amd64&&go build -trimpath -ldflags='-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative' -o $(BUILD_PATH)/$(PACKAGE_NAME)/bin/kuiperd.exe cmd/kuiperd/main.go'
endif

ifeq ($(uname_S), Linux)
	@export GO111MODULE=on; export CGO_ENABLED=0; export GOOS=windows; export GOARCH=amd64; go build -trimpath -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -o $(BUILD_PATH)/$(PACKAGE_NAME)/bin/kuiper.exe cmd/kuiper/main.go
	@export GO111MODULE=on; export CGO_ENABLED=1; export GOOS=windows; export GOARCH=amd64; go build -trimpath -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -o $(BUILD_PATH)/$(PACKAGE_NAME)/bin/kuiperd.exe cmd/kuiperd/main.go

	@export GO111MODULE=on; export CGO_ENABLED=0; export GOOS=linux; export GOARCH=amd64; go   build -trimpath -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -o $(BUILD_PATH)/$(PACKAGE_NAME)/bin/kuiper cmd/kuiper/main.go
	@export GO111MODULE=on; export CGO_ENABLED=1; export GOOS=linux; export GOARCH=amd64; go   build -trimpath -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -o $(BUILD_PATH)/$(PACKAGE_NAME)/bin/kuiperd cmd/kuiperd/main.go

	@export GO111MODULE=on; export CGO_ENABLED=0; export GOOS=linux; export GOARCH=arm64; go   build -trimpath -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -o $(BUILD_PATH)/$(PACKAGE_NAME)/bin/kuiper.arm64 cmd/kuiper/main.go
	@export GO111MODULE=on; export CGO_ENABLED=1; export GOOS=linux; export GOARCH=arm64; go   build -trimpath -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -o $(BUILD_PATH)/$(PACKAGE_NAME)/bin/kuiperd.arm64 cmd/kuiperd/main.go
endif

build_prepare:
ifeq ($(uname_S), Windows)
	@powershell -Command "mkdir -Force $(BUILD_PATH)\$(PACKAGE_NAME)\bin | out-null"
	@powershell -Command "mkdir -Force $(BUILD_PATH)\$(PACKAGE_NAME)\etc | out-null"
	@powershell -Command "mkdir -Force $(BUILD_PATH)\$(PACKAGE_NAME)\etc\sources | out-null"
	@powershell -Command "mkdir -Force $(BUILD_PATH)\$(PACKAGE_NAME)\etc\sinks | out-null"
	@powershell -Command "mkdir -Force $(BUILD_PATH)\$(PACKAGE_NAME)\etc\services | out-null"
	@powershell -Command "mkdir -Force $(BUILD_PATH)\$(PACKAGE_NAME)\etc\services\schemas | out-null"
	@powershell -Command "mkdir -Force $(BUILD_PATH)\$(PACKAGE_NAME)\data | out-null"
	@powershell -Command "mkdir -Force $(BUILD_PATH)\$(PACKAGE_NAME)\plugins | out-null"
	@powershell -Command "mkdir -Force $(BUILD_PATH)\$(PACKAGE_NAME)\plugins\sources | out-null"
	@powershell -Command "mkdir -Force $(BUILD_PATH)\$(PACKAGE_NAME)\plugins\sinks | out-null"
	@powershell -Command "mkdir -Force $(BUILD_PATH)\$(PACKAGE_NAME)\plugins\functions | out-null"
	@powershell -Command "mkdir -Force $(BUILD_PATH)\$(PACKAGE_NAME)\plugins\portable | out-null"
	@powershell -Command "mkdir -Force $(BUILD_PATH)\$(PACKAGE_NAME)\plugins\wasm | out-null"
	@powershell -Command "mkdir -Force $(BUILD_PATH)\$(PACKAGE_NAME)\log | out-null"

	@powershell -Command "Copy-Item -Force -Recurse etc\* $(BUILD_PATH)\$(PACKAGE_NAME)\etc | out-null"
else
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc/sources
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc/sinks
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc/services
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc/services/schemas
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/data
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/sources
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/sinks
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/functions
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/portable
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/wasm
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/log

	@cp -r etc/* $(BUILD_PATH)/$(PACKAGE_NAME)/etc
endif



run:
ifeq ($(uname_S), Windows)
	@echo "$(BUILD_PATH)/$(PACKAGE_NAME)/bin/kuiperd running "
	@powershell "$$env:KuiperBaseKey=\"${KUIPER_SOURCE}\$(BUILD_PATH)\$(PACKAGE_NAME)\"; &${KUIPER_SOURCE}\$(BUILD_PATH)\$(PACKAGE_NAME)\bin\kuiperd.exe $(ARGS)"
else 
	export KuiperBaseKey="${KUIPER_SOURCE}/$(BUILD_PATH)/$(PACKAGE_NAME)" && ${KUIPER_SOURCE}/$(BUILD_PATH)/$(PACKAGE_NAME)/bin/kuiperd $(ARGS)
endif