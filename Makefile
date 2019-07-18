GO 	   := GO111MODULE=on go
GOTEST := $(GO) test

FAILPOINT_ENABLE  := $$(find $$PWD/ -type d | grep -vE "(\.git|tools)" | xargs tools/failpoint-ctl enable)
FAILPOINT_DISABLE := $$(find $$PWD/ -type d | grep -vE "(\.git|tools)" | xargs tools/failpoint-ctl disable)

test: failpoint-enable
	$(GOTEST) ./... -cover || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)

cover: failpoint-enable
	$(GOTEST) ./... -coverprofile=cover.out || { $(FAILPOINT_DISABLE); exit 1; }
	go tool cover -html=cover.out
	@$(FAILPOINT_DISABLE)

bench:
	$(GOTEST) ./... -bench . -run 'skip-test'

failpoint-enable: tools/failpoint-ctl
	@$(FAILPOINT_ENABLE)

failpoint-disable: tools/failpoint-ctl
	@$(FAILPOINT_DISABLE)

tools/failpoint-ctl:
	$(GO) build -o $@ github.com/pingcap/failpoint/failpoint-ctl
