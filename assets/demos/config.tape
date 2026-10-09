# Shared settings, included by every tape with `Source`. Each tape starts from
# fresh demo data (setup.sh), hidden from the recording.
Set Shell "bash"
Set FontFamily "JetBrainsMono Nerd Font Mono"
Set FontSize 18
Set Width 1400
Set Height 760
Set Padding 24
Set Theme "Catppuccin Mocha"
Set WindowBar Colorful
Set TypingSpeed 35ms
Set CursorBlink false
Set WaitTimeout 90s
Set WaitPattern /❯\s*$/

Hide
Type "source assets/demos/env.sh && assets/demos/setup.sh && clear" Enter
Wait
Show
