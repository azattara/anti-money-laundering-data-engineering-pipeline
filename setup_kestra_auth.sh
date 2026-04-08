#!/bin/bash
set -ex

# Stop existing container
sudo docker stop kestra || true
sudo docker rm kestra || true

# Update config to disable basic auth (Kestra v1.3.7 OSS basicAuthService bug)
sudo tee /tmp/kestra-config.yml > /dev/null << 'EOF'
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
      enabled: false
    worker-task-timeout: PT3H
micronaut:
  security:
    enabled: false
EOF

# Restart Kestra with updated config and SA key mount
sudo docker run -d \
  --name kestra \
  --restart unless-stopped \
  --user root \
  --memory 12g \
  --memory-swap 12g \
  -e JAVA_OPTS="-Xms2g -Xmx4g -XX:MaxMetaspaceSize=512m" \
  -p 8080:8080 \
  -v /opt/kestra/db:/opt/kestra/db \
  -v /opt/kestra/storage:/opt/kestra/storage \
  -v /tmp/kestra-config.yml:/etc/kestra/config.yml:ro \
  -v /etc/docker/key.json:/app/secrets/key.json:ro \
  kestra/kestra:latest \
  server standalone --config /etc/kestra/config.yml

echo "=== Kestra restarted. Waiting for it to come up... ==="
for i in $(seq 1 30); do
  if sudo curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/v1/configs | grep -q 200; then
    echo "Kestra is UP"
    exit 0
  fi
  echo "Waiting... ($i/30)"
  sleep 5
done
echo "Timeout waiting for Kestra"
