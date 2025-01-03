VERSION := $(shell git rev-parse --short HEAD)

image:
	DOCKER_BUILDKIT=1 docker build -t cfs:${VERSION} .
