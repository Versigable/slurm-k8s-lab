#!/usr/bin/env bash
# One-time, idempotent Proxmox setup for the lab. Run as root on the PVE node (rogue).
#
# Creates:
#   - resource pool       slurm-k8s-lab
#   - Debian 13 template  VM 139 (in the pool), built from the verified cloud image
#   - role                LabTerraform (VM lifecycle only, no Sys.Modify, no root paths)
#   - user + API token    terraform@pve!lab, scoped by ACL to the pool, two datastores,
#                         the vmbr0 SDN bridge, and read-only on the node
#
# The token secret is written to $TOKEN_FILE (0600) and never printed.
set -euo pipefail

POOL=slurm-k8s-lab
NODE=$(hostname)
TEMPLATE_VMID=139
TEMPLATE_NAME=debian13-lab-template
VM_STORE=local-lvm
IMG_DIR=/var/lib/vz/template/iso
IMG_NAME=debian-13-genericcloud-amd64.qcow2
IMG_BASE=https://cloud.debian.org/images/cloud/trixie/latest
ROLE=LabTerraform
TF_USER=terraform@pve
TOKEN_ID=lab
TOKEN_FILE=/root/slurm-k8s-lab.tfapi

log() { printf '==> %s\n' "$*"; }

[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 1; }

# --- pool --------------------------------------------------------------------
if ! pvesh get /pools --output-format json | python3 -c "import json,sys; sys.exit(0 if any(p['poolid']=='$POOL' for p in json.load(sys.stdin)) else 1)"; then
  log "creating pool $POOL"
  pveum pool add "$POOL" --comment "slurm-k8s-lab: disposable learning lab, managed by Terraform"
fi

# --- cloud image (checksum-verified) -------------------------------------------
if [[ ! -f $IMG_DIR/$IMG_NAME ]]; then
  log "downloading $IMG_NAME"
  tmp=$(mktemp -d)
  curl -fsSL -o "$tmp/$IMG_NAME" "$IMG_BASE/$IMG_NAME"
  curl -fsSL -o "$tmp/SHA512SUMS" "$IMG_BASE/SHA512SUMS"
  (cd "$tmp" && grep " $IMG_NAME\$" SHA512SUMS | sha512sum -c -)
  mv "$tmp/$IMG_NAME" "$IMG_DIR/$IMG_NAME"
  rm -rf "$tmp"
fi

# --- template ----------------------------------------------------------------
if ! qm status "$TEMPLATE_VMID" >/dev/null 2>&1; then
  log "building template $TEMPLATE_VMID"
  qm create "$TEMPLATE_VMID" --name "$TEMPLATE_NAME" --pool "$POOL" \
    --machine q35 --cpu host --cores 1 --memory 1024 --balloon 0 \
    --net0 virtio,bridge=vmbr0 --scsihw virtio-scsi-single \
    --serial0 socket --ostype l26 --agent enabled=0 --onboot 0 \
    --tags "lab;template" \
    --description "slurm-k8s-lab base template (Debian 13 genericcloud). Built by scripts/proxmox-bootstrap.sh."
  qm set "$TEMPLATE_VMID" --virtio0 "$VM_STORE:0,import-from=$IMG_DIR/$IMG_NAME,discard=on,iothread=1"
  qm set "$TEMPLATE_VMID" --ide2 "$VM_STORE:cloudinit" --boot order=virtio0
  qm template "$TEMPLATE_VMID"
fi

# --- role --------------------------------------------------------------------
PRIVS="VM.Allocate VM.Clone VM.Audit VM.PowerMgmt VM.Config.CDROM VM.Config.Cloudinit VM.Config.CPU VM.Config.Disk VM.Config.HWType VM.Config.Memory VM.Config.Network VM.Config.Options Datastore.AllocateSpace Datastore.Audit Pool.Audit SDN.Use"
if pveum role list --output-format json | python3 -c "import json,sys; sys.exit(0 if any(r['roleid']=='$ROLE' for r in json.load(sys.stdin)) else 1)"; then
  pveum role modify "$ROLE" --privs "${PRIVS// /,}"
else
  log "creating role $ROLE"
  pveum role add "$ROLE" --privs "${PRIVS// /,}"
fi

# --- user + ACLs -------------------------------------------------------------
if ! pveum user list --output-format json | python3 -c "import json,sys; sys.exit(0 if any(u['userid']=='$TF_USER' for u in json.load(sys.stdin)) else 1)"; then
  log "creating user $TF_USER (no password; token-only)"
  pveum user add "$TF_USER" --comment "Terraform for slurm-k8s-lab (pool-scoped)"
fi

pveum acl modify "/pool/$POOL"                   --users "$TF_USER" --roles "$ROLE"
pveum acl modify "/storage/$VM_STORE"            --users "$TF_USER" --roles "$ROLE"
pveum acl modify "/sdn/zones/localnetwork/vmbr0" --users "$TF_USER" --roles "$ROLE"
pveum acl modify "/nodes/$NODE"                  --users "$TF_USER" --roles PVEAuditor

# --- token (privsep off: the token gets exactly the user's scoped ACLs) ---------
if [[ -s $TOKEN_FILE ]]; then
  log "token file already exists at $TOKEN_FILE; not rotating"
else
  pveum user token remove "$TF_USER" "$TOKEN_ID" >/dev/null 2>&1 || true
  umask 077
  pveum user token add "$TF_USER" "$TOKEN_ID" --privsep 0 --comment "slurm-k8s-lab terraform" --output-format json \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['full-tokenid']+'='+d['value'])" > "$TOKEN_FILE"
  log "token written to $TOKEN_FILE (0600). Export it as PROXMOX_VE_API_TOKEN on the control node."
fi

log "done"
