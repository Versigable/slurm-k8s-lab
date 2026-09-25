output "nodes" {
  description = "Lab nodes that currently exist."
  value = {
    for name, n in local.enabled_nodes : name => {
      vmid  = n.vmid
      ip    = n.ip
      group = n.group
    }
  }
}
