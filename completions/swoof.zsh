#compdef swoof

# zsh completion for swoof
#
# Candidates are produced by `swoof __complete`, which reads the user's
# connections/aliases files and the table-name cache, so completions cover
# connection names, alias names, and (cached) table names — not just flags.

_swoof() {
    local cur
    local -a candidates
    cur="${words[CURRENT]}"
    candidates=(${(f)"$(swoof __complete ${words[2,CURRENT-1]} "$cur" 2>/dev/null)"})
    compadd -- $candidates
}

_swoof "$@"
