#!/bin/bash -eux

pushd dp-search-reindex-batch
  make build
  cp build/dp-search-reindex-batch Dockerfile.concourse ../build
popd
