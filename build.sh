#!/bin/bash -e

error() {
  printf '\E[31m'; echo "$@" ; printf '\E[0m'
}

export TAG="${1:-latest}"
export ARCH="${2:-$(uname -m)}"
export PLUGIN_PREFIX="${PLUGIN_PREFIX:-ghcr.io/${GITHUB_REPOSITORY,,}}"

build() {
  if [ "$ARCH" = "armv7l" ]; then
    BPLATFORM="linux/arm/v7"
  elif [ "$ARCH" = "aarch64" ]; then
    BPLATFORM="linux/arm64"
  else
    BPLATFORM="linux/amd64"
  fi

  if [ "$ARCH" = "x86_64" ]; then
    docker plugin rm -f "$PLUGIN_PREFIX:$TAG" || true
  else
    docker plugin rm -f "$PLUGIN_PREFIX-$ARCH:$TAG" || true
  fi

  docker rmi -f rootfsimage || true

  docker buildx build --load --platform "${BPLATFORM}" \
    --build-arg GO_VERSION=1.15.10 \
    --build-arg UBUNTU_VERSION=22.04 \
    -t rootfsimage -f "$1/Dockerfile" .

  id=$(docker create rootfsimage true)
  rm -rf build/rootfs
  mkdir -p build/rootfs/var/lib/docker-volumes
  docker export "$id" | tar -x -C build/rootfs
  docker rm -vf "$id"

  cp "$1/config.json" build

  if [ "$ARCH" = "x86_64" ]; then
    docker plugin create "$PLUGIN_PREFIX:$TAG" build
    docker plugin push "$PLUGIN_PREFIX:$TAG"
  else
    docker plugin create "$PLUGIN_PREFIX-$ARCH:$TAG" build
    docker plugin push "$PLUGIN_PREFIX-$ARCH:$TAG"
  fi
}

build src
