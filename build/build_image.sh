#!/usr/bin/env bash

./build/build.sh

buildah from --name puzzlewikiserver-working-container scratch
buildah copy puzzlewikiserver-working-container $HOME/go/bin/puzzlewikiserver /bin/puzzlewikiserver
buildah config --env SERVICE_PORT=50051 puzzlewikiserver-working-container
buildah config --port 50051 puzzlewikiserver-working-container
buildah config --entrypoint '["/bin/puzzlewikiserver"]' puzzlewikiserver-working-container
buildah commit puzzlewikiserver-working-container puzzlewikiserver
buildah rm puzzlewikiserver-working-container

buildah push puzzlewikiserver docker-daemon:puzzlewikiserver:latest
