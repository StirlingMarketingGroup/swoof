# bash completion for swoof
#
# Candidates are produced by `swoof __complete`, which reads the user's
# connections/aliases files and the table-name cache, so completions cover
# connection names, alias names, and (cached) table names — not just flags.

_swoof() {
    local cur candidates
    cur="${COMP_WORDS[COMP_CWORD]}"
    candidates="$(swoof __complete "${COMP_WORDS[@]:1:COMP_CWORD-1}" "$cur" 2>/dev/null)"
    COMPREPLY=($(compgen -W "${candidates}" -- "${cur}"))
}
complete -F _swoof swoof
