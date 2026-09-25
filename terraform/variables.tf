variable "proxmox_endpoint" {
  description = "Proxmox VE API endpoint."
  type        = string
  default     = "https://10.0.2.1:8006/"
}

variable "proxmox_insecure" {
  description = "Skip TLS verification (the homelab PVE uses its self-signed cert)."
  type        = bool
  default     = true
}

variable "node_name" {
  description = "Proxmox node that hosts every lab VM."
  type        = string
  default     = "rogue"
}

variable "pool_id" {
  description = "Proxmox resource pool the lab VMs live in. The Terraform token is scoped to it."
  type        = string
  default     = "slurm-k8s-lab"
}

variable "vm_datastore" {
  description = "Datastore for VM disks and cloud-init drives."
  type        = string
  default     = "local-lvm"
}

variable "template_vmid" {
  description = "Debian 13 cloud-init template created by scripts/proxmox-bootstrap.sh."
  type        = number
  default     = 139
}

variable "network_bridge" {
  type    = string
  default = "vmbr0"
}

variable "network_prefix" {
  description = "X-Mansion is one flat /16."
  type        = number
  default     = 16
}

variable "gateway" {
  type    = string
  default = "10.0.1.1"
}

variable "dns_servers" {
  type    = list(string)
  default = ["10.0.1.1"]
}

variable "dns_domain" {
  type    = string
  default = "mutants.xmen"
}

variable "admin_user" {
  description = "Login user created by cloud-init on every VM (passwordless sudo, key-only SSH)."
  type        = string
  default     = "labadmin"
}

variable "ssh_public_keys" {
  description = "Public keys authorized for admin_user."
  type        = list(string)
}

variable "enable_slurm" {
  description = "Create the classic Slurm VMs (130-132)."
  type        = bool
  default     = true
}

variable "enable_k8s" {
  description = "Create the kubeadm VMs (133-135). Off until Phase 2."
  type        = bool
  default     = false
}

variable "started" {
  description = "Power state. Set false to park the lab without destroying it."
  type        = bool
  default     = true
}
