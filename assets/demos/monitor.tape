# Live topic throughput, then a cluster health check.
Output assets/demos/monitor.gif
Source assets/demos/config.tape

Hide
Type `(ntk produce shop.orders -k 'order-{{.i}}' -v '{"status":"NEW"}' --template --repeat --rate 80/s >/dev/null 2>&1 & echo $! >/tmp/ntk-demo-load.pid); sleep 3; clear` Enter
Wait
Show

Type "ntk topic watch shop.orders --interval 1s" Sleep 300ms Enter Sleep 9s
Ctrl+C Sleep 600ms
Type "clear" Enter
Type "ntk health" Sleep 300ms Enter Wait Sleep 4s

Hide
Type "kill $(cat /tmp/ntk-demo-load.pid); rm /tmp/ntk-demo-load.pid" Enter
