#!/bin/bash
set -e

: "${KESTRA_BASIC_AUTH_USERNAME:?Set KESTRA_BASIC_AUTH_USERNAME before running this script}"
: "${KESTRA_BASIC_AUTH_PASSWORD:?Set KESTRA_BASIC_AUTH_PASSWORD before running this script}"

docker rm -f kestra 2>/dev/null || true

# Create persistent dirs on the VM host
sudo mkdir -p /opt/kestra/plugins /opt/kestra/db /opt/kestra/storage
sudo chown -R $USER:$USER /opt/kestra

# Start Kestra with persistent volumes mounted
docker run -d \
  --name kestra \
  --restart unless-stopped \
  -p 8080:8080 \
  -v /opt/kestra/plugins:/app/plugins \
  -v /opt/kestra/db:/opt/kestra/db \
  -v /opt/kestra/storage:/opt/kestra/storage \
  -e KESTRA_CONFIGURATION='
datasources:
  h2:
    url: "jdbc:h2:/opt/kestra/db/db;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE"
    driverClassName: org.h2.Driver
    username: sa
    password: ""
kestra:
  repository:
    type: h2
  queue:
    type: h2
  storage:
    type: local
    local:
      base-path: /opt/kestra/storage
  tasks:
    tmp-dir:
      path: /tmp/kestra-wd/tmp
  server:
    basic-auth:
      enabled: true
      username: "'"$KESTRA_BASIC_AUTH_USERNAME"'"
      password: "'"$KESTRA_BASIC_AUTH_PASSWORD"'"
micronaut:
  security:
    enabled: true
' \
  kestra/kestra:latest \
  server standalone --plugins /app/plugins

echo "Kestra container started, plugins loaded from /app/plugins"
