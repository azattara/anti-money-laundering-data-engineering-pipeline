#!/bin/bash
set -euxo pipefail

# Install pip if missing
if ! command -v pip3 &>/dev/null; then
  sudo apt-get update -y
  sudo apt-get install -y python3-pip python3-venv
fi

# Install dbt in a venv at /opt/dbt-venv
sudo rm -rf /opt/dbt-venv
sudo python3 -m venv /opt/dbt-venv
sudo /opt/dbt-venv/bin/pip install --quiet dbt-core==1.8.2 dbt-bigquery==1.8.2

# Symlink dbt to PATH
sudo ln -sf /opt/dbt-venv/bin/dbt /usr/local/bin/dbt
dbt --version

# Clone the dbt project from GCS artifacts
sudo mkdir -p /app/dbt
gsutil -m cp gs://anti-ml-data-engineering-artifacts/dbt/* /tmp/dbt-staging/ 2>/dev/null || true
gsutil -m rsync -r gs://anti-ml-data-engineering-artifacts/dbt/ /app/dbt/

# Create profiles.yml
sudo tee /app/dbt/profiles.yml > /dev/null <<'EOF'
anti_money_laundering:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: service-account
      project: anti-ml-data-engineering
      dataset: aml_gold
      threads: 8
      timeout_seconds: 300
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      location: US
EOF

echo "=== dbt setup complete ==="
ls -la /app/dbt/
