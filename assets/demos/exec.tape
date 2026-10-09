# Consume into a command per message: data on stdin, metadata in NTK_* variables.
Output assets/demos/exec.gif
Source assets/demos/config.tape

Type `ntk consume shop.orders --from -8 -o 'exec:if grep -q FAILED; then echo "$NTK_KEY failed (partition $NTK_PARTITION, offset $NTK_OFFSET)"; fi'` Sleep 400ms Enter Wait Sleep 3.5s
Type `ntk consume shop.orders --from -1 -o 'exec:sleep 0.3' --exec-parallel 3 --receipt` Sleep 400ms Enter Wait Sleep 3.5s
