# allow overriding gradle command binary
# use OS version if present and use gradlew as fallback
ifeq (${GRADLE_COMMAND},)
	GRADLE_COMMAND_OS=$(shell which gradle)
	ifeq (${GRADLE_COMMAND_OS},)
		GRADLE_COMMAND="./gradlew"
	else
		GRADLE_COMMAND=${GRADLE_COMMAND_OS}
	endif
endif

GITVERSION=$(shell git describe --always --dirty)
WEBAPPTARGET=webapp/build/libs/elds-backend-${GITVERSION}.war

.PHONY: build docker

default: help

## Build the application with the default profile
build:
	${GRADLE_COMMAND} clean build -x test

## Apply all check tasks with the default profile(as well as project report & coverage report) with the default profile
check:
	${GRADLE_COMMAND} clean check -x test

## this target is used as a bamboo plan main stage job
bamboo-check: check

## this target is used as a bamboo plan main stage job
bamboo-build: build

## Show this help screen
help:
	@printf "Available targets\n\n"
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			printf "%-15s %s\n", helpCommand, helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)
