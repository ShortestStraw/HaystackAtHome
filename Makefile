VERSION_FILE := Vers.ion
VERSION ?= $(shell cat $(VERSION_FILE))
VERSION_VAR ?= HaystackAtHome/internal/build_info.version

BIN_DIR ?= $(abspath bin)
PROGS := gw ss client

.PHONY: version-bump-patch version-bump-minor version-bump-major build clean build-grpc test bench

# Helper to ensure the version file exists before bumping
init-version:
	@if [ ! -f $(VERSION_FILE) ]; then echo "0.0.0" > $(VERSION_FILE); fi

version-bump-patch: init-version
	@MAJOR=$$(echo $(VERSION) | cut -d. -f1); \
	MINOR=$$(echo $(VERSION) | cut -d. -f2); \
	PATCH=$$(echo $(VERSION) | cut -d. -f3); \
	NEW_VERSION="$$MAJOR.$$MINOR.$$(($$PATCH + 1))"; \
	echo $$NEW_VERSION > $(VERSION_FILE); \
	echo "Bumped Patch: $(VERSION) -> $$NEW_VERSION"

version-bump-minor: init-version
	@MAJOR=$$(echo $(VERSION) | cut -d. -f1); \
	MINOR=$$(echo $(VERSION) | cut -d. -f2); \
	NEW_VERSION="$$MAJOR.$$(($$MINOR + 1)).0"; \
	echo $$NEW_VERSION > $(VERSION_FILE); \
	echo "Bumped Minor: $(VERSION) -> $$NEW_VERSION"

version-bump-major: init-version
	@MAJOR=$$(echo $(VERSION) | cut -d. -f1); \
	NEW_VERSION="$$(($$MAJOR + 1)).0.0"; \
	echo $$NEW_VERSION > $(VERSION_FILE); \
	echo "Bumped Major: $(VERSION) -> $$NEW_VERSION"

bin_dir: $(BIN_DIR)
	mkdir -p $(BIN_DIR)

PHASE=0

build-grpc:
	$(MAKE) -C internal/transport build

build: clean bin_dir build-grpc
	for prog in $(PROGS); do \
		go build -ldflags="-X '$(VERSION_VAR)=$(VERSION)'" -o $(BIN_DIR)/$$prog cmd/$$prog/main.go; \
	done

clean:
	$(MAKE) -C internal/transport clean
	rm -rf $(BIN_DIR)/*

test:
ifdef TEST
	TEST_LOG=1 go test -timeout 20s -v -cover -run $(TEST) $(or $(PKG),./...)
else
	go test -timeout 20s -v -cover ./...
endif

bench:
	go test -timeout 600s -bench=. -run='^$$' ./internal/ss/storage/needle/