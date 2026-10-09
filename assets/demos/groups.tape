# Consumer groups: lag, members and offsets, a dry-run offset reset, and a lag check for scripts.
Output assets/demos/groups.gif
Source assets/demos/config.tape

Type "ntk group list --lag" Sleep 300ms Enter Wait Sleep 2.5s
Type "ntk group describe billing" Sleep 300ms Enter Wait Sleep 3s
Type "clear" Enter
Type "ntk group reset billing --topic shop.orders --to-earliest" Sleep 300ms Enter Wait Sleep 4s
Type `ntk group lag billing --threshold 10; echo "exit code $?"` Sleep 300ms Enter Wait Sleep 3.5s
