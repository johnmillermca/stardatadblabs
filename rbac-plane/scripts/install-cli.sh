#!/usr/bin/env bash
# install-cli.sh — installs the rbacctl CLI into ~/.local/bin
# Usage: bash rbac-plane/scripts/install-cli.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RBAC_PLANE_DIR="$(dirname "${SCRIPT_DIR}")"
CLI_SRC="${RBAC_PLANE_DIR}/cli/rbacctl.py"
INSTALL_DIR="${HOME}/.local/bin"
WRAPPER="${INSTALL_DIR}/rbacctl"

echo "Installing rbacctl CLI dependencies..."
pip3 install --user --quiet \
  "typer==0.15.3" "click==8.1.8" "httpx==0.28.1" "rich==13.9.4"

mkdir -p "${INSTALL_DIR}"

cat > "${WRAPPER}" <<EOF
#!/usr/bin/env bash
exec python3 "${CLI_SRC}" "\$@"
EOF
chmod +x "${WRAPPER}"

echo "✓ rbacctl installed → ${WRAPPER}"
echo ""

# Ensure ~/.local/bin is on PATH for this shell and future sessions
if ! echo "${PATH}" | grep -q "${INSTALL_DIR}"; then
    echo "  Adding ${INSTALL_DIR} to PATH in ~/.bashrc"
    echo 'export PATH="${HOME}/.local/bin:${PATH}"' >> "${HOME}/.bashrc"
    export PATH="${INSTALL_DIR}:${PATH}"
    echo "  Run: source ~/.bashrc   (or open a new terminal)"
fi

echo ""
echo "Test with:"
echo "  export RBAC_URL=http://192.168.1.50:30850"
echo "  export RBAC_TOKEN=\$(kubectl get secret rbac-plane-credentials -n prod \\"
echo "    -o jsonpath='{.data.MASTER_TOKEN}' | base64 -d)"
echo "  rbacctl --help"
