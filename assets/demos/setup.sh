#!/usr/bin/env bash
# Resets the shop.* demo topics and groups on the sandbox, so every tape renders the same.
set -euo pipefail
cd "$(dirname "$0")/../.."
export NTK_PROFILE_PATH=$PWD/assets/demos/profiles.json
ntk() { ./bin/ntk "$@"; }

ntk g delete billing shipping -y >/dev/null 2>&1 || true
existing=$(ntk t list 'shop.*' -o name)
if [ -n "$existing" ]; then
  # shellcheck disable=SC2086
  ntk t delete $existing -y >/dev/null 2>&1
fi
ntk t create shop.orders shop.orders.mirror --partitions 6 -y >/dev/null 2>&1
ntk t create shop.payments shop.shipments --partitions 3 -y >/dev/null 2>&1

awk 'BEGIN {
  split("NEW PAID PAID SHIPPED SHIPPED SHIPPED FAILED", s, " "); srand(7)
  for (i = 1001; i <= 1240; i++)
    printf "order-%d:{\"id\":\"order-%d\",\"customer\":\"c-%03d\",\"amount\":%.2f,\"status\":\"%s\"}\n",
      i, i, int(rand() * 300), rand() * 500 + 5, s[int(rand() * 7) + 1]
}' | ntk p shop.orders --key-sep : -H source=checkout -H tenant=acme 2>/dev/null

awk 'BEGIN { srand(11); for (i = 1001; i <= 1120; i++)
  printf "order-%d:{\"order\":\"order-%d\",\"amount\":%.2f,\"method\":\"card\"}\n", i, i, rand() * 500 + 5 }' |
  ntk p shop.payments --key-sep : 2>/dev/null
awk 'BEGIN { for (i = 1001; i <= 1080; i++) printf "order-%d:{\"order\":\"order-%d\",\"carrier\":\"postnord\"}\n", i, i }' |
  ntk p shop.shipments --key-sep : 2>/dev/null

ntk c shop.orders --from earliest -g billing -n 180 -q >/dev/null
ntk c shop.shipments --from earliest -g shipping -n 80 -q >/dev/null
