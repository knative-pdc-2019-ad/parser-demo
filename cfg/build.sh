#!/usr/bin/env bash

./gradlew clean jib \
  --image=skylab00/parser:v01 \
  -Djib.to.auth.username=$DOCKER_HUB_ID \
  -Djib.to.auth.password=$DOCKER_HUB_PASS