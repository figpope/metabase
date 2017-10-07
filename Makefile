SHELL := /bin/bash

.PHONY: build-docker
build-docker:
	./bin/docker/build_image.sh source 0.24.2-celmatix
	docker tag metabase/metabase-head:0.24.2-celmatix localhost:5000/celmatix/metabase:v0.24.2-celmatix

.PHONY: deploy-docker
deploy-docker: build-docker
	docker push localhost:5000/celmatix/metabase:v0.24.2-celmatix