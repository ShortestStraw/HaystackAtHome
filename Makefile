VERSION_FILE := Vers.ion

.PHONY: version-bump-patch version-bump-minor version-bump-major

# Helper to ensure the version file exists before bumping
init-version:
	@if [ ! -f $(VERSION_FILE) ]; then echo "0.0.0" > $(VERSION_FILE); fi

version-bump-patch: init-version
	@VERSION=$$(cat $(VERSION_FILE)); \
	MAJOR=$$(echo $$VERSION | cut -d. -f1); \
	MINOR=$$(echo $$VERSION | cut -d. -f2); \
	PATCH=$$(echo $$VERSION | cut -d. -f3); \
	NEW_VERSION="$$MAJOR.$$MINOR.$$(($$PATCH + 1))"; \
	echo $$NEW_VERSION > $(VERSION_FILE); \
	echo "Bumped Patch: $$VERSION -> $$NEW_VERSION"

version-bump-minor: init-version
	@VERSION=$$(cat $(VERSION_FILE)); \
	MAJOR=$$(echo $$VERSION | cut -d. -f1); \
	MINOR=$$(echo $$VERSION | cut -d. -f2); \
	NEW_VERSION="$$MAJOR.$$(($$MINOR + 1)).0"; \
	echo $$NEW_VERSION > $(VERSION_FILE); \
	echo "Bumped Minor: $$VERSION -> $$NEW_VERSION"

version-bump-major: init-version
	@VERSION=$$(cat $(VERSION_FILE)); \
	MAJOR=$$(echo $$VERSION | cut -d. -f1); \
	NEW_VERSION="$$(($$MAJOR + 1)).0.0"; \
	echo $$NEW_VERSION > $(VERSION_FILE); \
	echo "Bumped Major: $$VERSION -> $$NEW_VERSION"