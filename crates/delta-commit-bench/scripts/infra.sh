#!/usr/bin/env bash
# Benchmark infrastructure, following the hybrid-vm-migration security initiative on the dev subscription:
# - Storage: shared key and anonymous access off, Entra RBAC only, firewall default deny with a VNet
#   service endpoint rule, and an NSP (Enforced) whose only inbound rule is this subscription.
# - Compute: a Flexible VMSS (standalone VMs are denied), Trusted Launch, user-assigned identity,
#   no public IPs, NSG with no inbound rules, SSH only through Azure Bastion tunnelling.
# - Public IPs (Bastion, NAT) carry the FirstPartyUsage=/NonProd IP tag required by policy.
#
# Usage: scripts/infra.sh up|ssh <n>|tunnel <n> <port>|stop|start|down
set -euo pipefail

SUB=${SUB:-3afca1ee-abcc-4e4b-be8c-d3a273db45d3}
# Storage accounts are restricted to an allowed-region list by policy; westus3 is not on it.
LOC=${LOC:-westus2}
RG=${RG:-ethanmoffat-arrowcol-delta-bench}
P=${P:-ethanmoffat-dcb}
VM_SKU=${VM_SKU:-Standard_D16s_v5}
VM_COUNT=${VM_COUNT:-3}
SSH_KEY=${SSH_KEY:-$HOME/.ssh/dcb_bench}
SUFFIX=$(echo -n "$SUB/$RG" | md5sum | cut -c1-6)
SA_HNS=emdcbhns$SUFFIX
SA_FLAT=emdcbflat$SUFFIX
TAGS=(owner=ethanmoffat purpose=arrowcol-delta-bench)
IPTAG=FirstPartyUsage=/NonProd

az() { command az "$@" --subscription "$SUB" --only-show-errors; }

up() {
  az group create -n "$RG" -l "$LOC" --tags "${TAGS[@]}" -o none

  az network nsg create -g "$RG" -n "$P-nsg" --tags "${TAGS[@]}" -o none
  az network vnet create -g "$RG" -n "$P-vnet" --address-prefixes 10.60.0.0/16 \
    --tags "${TAGS[@]}" NRMSBastion=true -o none
  for name in nat bastion; do
    az network public-ip create -g "$RG" -n "$P-pip-$name" --sku Standard --allocation-method Static \
      --ip-tags "$IPTAG" --tags "${TAGS[@]}" -o none
  done
  az network nat gateway create -g "$RG" -n "$P-nat" --public-ip-addresses "$P-pip-nat" \
    --tags "${TAGS[@]}" -o none
  az network vnet subnet create -g "$RG" --vnet-name "$P-vnet" -n bench --address-prefixes 10.60.1.0/24 \
    --network-security-group "$P-nsg" --nat-gateway "$P-nat" --service-endpoints Microsoft.Storage \
    --default-outbound-access false -o none
  az network vnet subnet create -g "$RG" --vnet-name "$P-vnet" -n AzureBastionSubnet \
    --address-prefixes 10.60.2.0/26 -o none
  az network bastion create -g "$RG" -n "$P-bastion" --vnet-name "$P-vnet" --public-ip-address "$P-pip-bastion" \
    --sku Standard --enable-tunneling true -l "$LOC" --tags "${TAGS[@]}" -o none

  local subnet_id
  subnet_id=$(az network vnet subnet show -g "$RG" --vnet-name "$P-vnet" -n bench --query id -o tsv)
  for sa in "$SA_HNS:true" "$SA_FLAT:false"; do
    local name=${sa%%:*} hns=${sa##*:}
    az storage account create -g "$RG" -n "$name" -l "$LOC" --sku Standard_LRS --kind StorageV2 \
      --hns "$hns" --allow-shared-key-access false --allow-blob-public-access false --min-tls-version TLS1_2 \
      --https-only true --default-action Deny --bypass None --tags "${TAGS[@]}" -o none
    az storage account network-rule add -g "$RG" --account-name "$name" --subnet "$subnet_id" -o none
    az storage container-rm create -g "$RG" --storage-account "$name" -n bench -o none
  done

  az network perimeter create -g "$RG" -n "$P-nsp" -l "$LOC" --tags "${TAGS[@]}" -o none
  az network perimeter profile create -g "$RG" --perimeter-name "$P-nsp" -n bench -o none
  az network perimeter profile access-rule create -g "$RG" --perimeter-name "$P-nsp" --profile-name bench \
    -n allow-subscription --direction Inbound --subscriptions "[0].id=/subscriptions/$SUB" -o none
  local profile_id
  profile_id=$(az network perimeter profile show -g "$RG" --perimeter-name "$P-nsp" -n bench --query id -o tsv)
  for name in "$SA_HNS" "$SA_FLAT"; do
    az network perimeter association create -g "$RG" --perimeter-name "$P-nsp" -n "assoc-$name" \
      --access-mode Enforced --private-link-resource "{id:$(sa_id "$name")}" --profile "{id:$profile_id}" -o none
  done

  az identity create -g "$RG" -n "$P-id" --tags "${TAGS[@]}" -o none
  local principal identity_id
  principal=$(az identity show -g "$RG" -n "$P-id" --query principalId -o tsv)
  identity_id=$(az identity show -g "$RG" -n "$P-id" --query id -o tsv)
  for name in "$SA_HNS" "$SA_FLAT"; do
    az role assignment create --assignee-object-id "$principal" --assignee-principal-type ServicePrincipal \
      --role "Storage Blob Data Contributor" --scope "$(sa_id "$name")" -o none
  done

  [ -f "$SSH_KEY" ] || ssh-keygen -q -t ed25519 -N "" -C dcb-bench -f "$SSH_KEY"
  az vmss create -g "$RG" -n "$P-vmss" --orchestration-mode Flexible --vm-sku "$VM_SKU" \
    --instance-count "$VM_COUNT" --image Ubuntu2204 --security-type TrustedLaunch --enable-secure-boot true \
    --enable-vtpm true --admin-username azureuser --ssh-key-values "$SSH_KEY.pub" \
    --vnet-name "$P-vnet" --subnet bench --nsg "$P-nsg" --lb "" \
    --platform-fault-domain-count 1 --accelerated-networking true --assign-identity "$identity_id" \
    --tags "${TAGS[@]}" azsecpack=nonprod -o none

  echo "storage: $SA_HNS (HNS), $SA_FLAT (flat); identity client id: $(az identity show -g "$RG" -n "$P-id" --query clientId -o tsv)"
  vm_ids
}

sa_id() { az storage account show -g "$RG" -n "$1" --query id -o tsv; }
vm_ids() { az vm list -g "$RG" --query "[?contains(name,'$P-vmss')].{name:name,id:id}" -o tsv | sort; }
vm_id() { vm_ids | sed -n "${1}p" | cut -f2; }

case ${1:-} in
  up) up ;;
  ssh) shift; n=$1; shift
       command az network bastion ssh --subscription "$SUB" -g "$RG" -n "$P-bastion" --target-resource-id "$(vm_id "$n")" \
         --auth-type ssh-key --username azureuser --ssh-key "$SSH_KEY" -- "$@" ;;
  tunnel) az network bastion tunnel -g "$RG" -n "$P-bastion" --target-resource-id "$(vm_id "$2")" \
            --resource-port 22 --port "$3" ;;
  push|pull) op=$1; n=$2; shift 2; port=$((2200 + n))
        command az network bastion tunnel --subscription "$SUB" -g "$RG" -n "$P-bastion" \
          --target-resource-id "$(vm_id "$n")" --resource-port 22 --port "$port" >/dev/null 2>&1 &
        tpid=$!; trap 'kill $tpid 2>/dev/null' EXIT
        for _ in $(seq 60); do (exec 3<>/dev/tcp/127.0.0.1/$port) 2>/dev/null && break; sleep 1; done
        o=(-q -r -i "$SSH_KEY" -P "$port" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)
        if [ "$op" = push ]; then scp "${o[@]}" "$@" azureuser@127.0.0.1:
        else scp "${o[@]}" "azureuser@127.0.0.1:$1" "$2"; fi ;;
  stop) az vmss deallocate -g "$RG" -n "$P-vmss" -o none ;;
  start) az vmss start -g "$RG" -n "$P-vmss" -o none ;;
  down) az group delete -n "$RG" --yes ;;
  *) echo "usage: $0 up|ssh <n> [ssh args] cmd|tunnel <n> <port>|push <n> files...|pull <n> src dst|stop|start|down" >&2; exit 2 ;;
esac
