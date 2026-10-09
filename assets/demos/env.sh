# Sourced (hidden) at the start of every tape.
cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
export PATH=$PWD/bin:$PATH
export NTK_PROFILE_PATH=$PWD/assets/demos/profiles.json
export PS1='\[\e[1;35m\]❯\[\e[0m\] '
unset PROMPT_COMMAND
