# Mirror a topic over a Unix datagram socket, from one profile (cluster) to another.
Output assets/demos/mirror.gif
Source assets/demos/config.tape

Hide
Type "tmux -L ntk-demo -f assets/demos/tmux.conf new-session" Enter Sleep 1s
Type "tmux select-pane -T 'listener: ntk produce' && tmux split-window -v && tmux select-pane -T 'sender: ntk consume' && tmux select-pane -U && clear" Enter Sleep 1s
Type "clear" Enter
Show

Type "ntk -p local-scram produce shop.orders.mirror --in unix:/tmp/mirror.sock -m" Sleep 300ms Enter Sleep 1.5s
Ctrl+B Down Sleep 500ms
Type "ntk -p local consume shop.orders --from earliest -m -o unix:/tmp/mirror.sock --unix-wait" Sleep 300ms Enter Sleep 3s
Type "ntk consume shop.orders.mirror --key-partition order-1001 --from -1 -m" Sleep 300ms Enter Sleep 3.5s
Ctrl+B Up Sleep 500ms
Ctrl+C Sleep 3.5s

Hide
Type "tmux -L ntk-demo kill-server" Enter
