#!/usr/bin/env bash
set -euo pipefail

echo 'Start Oracle Instant Client installation!'

IC_DIR="/opt/oracle"
IC_VER_DIR="instantclient_21_6"
IC_URL="https://download.oracle.com/otn_software/linux/instantclient/216000/instantclient-basic-linux.x64-21.6.0.0.0dbru.zip"

apt-get update
export DEBIAN_FRONTEND=noninteractive

apt-get install -y --no-install-recommends curl unzip ca-certificates \
  && (apt-get install -y --no-install-recommends libaio1 || apt-get install -y --no-install-recommends libaio1t64) \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

# Debian 12 t64 compatibility: provide libaio.so.1 if only libaio.so.1t64 exists
if [ -f /usr/lib/x86_64-linux-gnu/libaio.so.1t64 ]; then
  ln -sf /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1
fi

echo 'Installing Oracle Instant Client ...'
mkdir -p "${IC_DIR}"
rm -rf "${IC_DIR:?}/"*
cd "${IC_DIR}"

curl -SL "${IC_URL}" -o instant_client.zip
unzip instant_client.zip
rm instant_client.zip

# Update runtime link path (so you don't rely on bashrc)
echo "${IC_DIR}/${IC_VER_DIR}" > /etc/ld.so.conf.d/oracle-instantclient.conf
ldconfig

# Optional: set NLS_LANG globally for shells (won't affect services unless they read it)
echo 'export NLS_LANG="AMERICAN_AMERICA.AL32UTF8"' >> /etc/bash.bashrc

echo "Done! Instant Client in ${IC_DIR}/${IC_VER_DIR}"
