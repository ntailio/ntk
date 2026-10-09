# ntk at a glance: inspect, produce, consume, then the TUI.
Output assets/demos/demo.gif
Source assets/demos/config.tape

Type "ntk topic list 'shop.*'" Sleep 300ms Enter Wait Sleep 2.5s
Type `ntk produce shop.orders -k order-2001 -v '{"id":"order-2001","status":"NEW"}' -H tenant=acme` Sleep 300ms Enter Wait Sleep 2s
Type "ntk consume shop.orders --key-partition order-2001 --from -3" Sleep 300ms Enter Wait Sleep 3s
Type "ntk group list --lag" Sleep 300ms Enter Wait Sleep 3s
Type "clear" Enter

# The TUI
Type "ntk" Sleep 300ms Enter Sleep 3s
Type "/shop" Sleep 800ms Enter Sleep 1.5s
Enter Sleep 3s
Tab Sleep 2.5s
Tab Sleep 2.5s
Escape Sleep 1s
Type ":consume shop.orders" Sleep 400ms Enter Sleep 3s
Down Sleep 300ms Down Sleep 300ms Down Sleep 1.2s
Tab Sleep 1.5s
Tab Sleep 1.5s
Escape Sleep 1s
Type ":groups" Sleep 400ms Enter Sleep 2.5s
Enter Sleep 3s
Escape Sleep 1s
Type ":health" Sleep 400ms Enter Sleep 3.5s
Type "q" Sleep 1s
