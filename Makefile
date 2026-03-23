VERSION_FILE := Vers.ion
VERSION ?= $(shell cat $(VERSION_FILE))
BIN_DIR ?= $(abspath bin)

.PHONY: version-bump-patch version-bump-minor version-bump-major build clean

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

build: bin_dir clean 
	@echo "--------------------------------------------------"
	@echo "[PHASE $(PHASE)] Building client"; $(eval PHASE = $(shell echo $$(($(PHASE) + 1))))
	$(MAKE) -C cmd/client build VERSION=$(VERSION) BIN_DIR=$(BIN_DIR)

	@echo "--------------------------------------------------"
	@echo "[PHASE $(PHASE)] Building grpc transport"; $(eval PHASE = $(shell echo $$(($(PHASE) + 1))))
	$(MAKE) -C internal/transport build BIN_DIR=$(BIN_DIR)

	@echo "--------------------------------------------------"
	@echo "[PHASE $(PHASE)] Building GW"; $(eval PHASE = $(shell echo $$(($(PHASE) + 1))))
	$(MAKE) -C cmd/gw build VERSION=$(VERSION) BIN_DIR=$(BIN_DIR)

	@echo "--------------------------------------------------"
	@echo "[PHASE $(PHASE)] Building SS"; $(eval PHASE = $(shell echo $$(($(PHASE) + 1))))
	$(MAKE) -C cmd/ss build VERSION=$(VERSION) BIN_DIR=$(BIN_DIR)

clean:
	@echo "--------------------------------------------------"
	@echo "[PHASE $(PHASE)] Building SS"; $(eval PHASE = $(shell echo $$(($(PHASE) + 1))))

	$(MAKE) -C cmd/client clean
	$(MAKE) -C internal/transport clean
	$(MAKE) -C cmd/gw clean
	$(MAKE) -C cmd/ss clean
	rm -rf $(BIN_DIR)/*