# Profiles: one file, several clusters, a staged connection test, read-only safety.
Output assets/demos/profiles.gif
Source assets/demos/config.tape

Type "ntk profile list" Sleep 300ms Enter Wait Sleep 2.5s
Type "ntk profile test local-scram" Sleep 300ms Enter Wait Sleep 3.5s
Type "ntk -p local-scram whoami" Sleep 300ms Enter Wait Sleep 3s
Type "ntk -p prod topic delete shop.orders" Sleep 300ms Enter Wait Sleep 3s
