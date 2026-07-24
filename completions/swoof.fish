# fish completion for swoof
#
# Candidates are produced by `swoof __complete`, which reads the user's
# connections/aliases files and the table-name cache, so completions cover
# connection names, alias names, and (cached) table names — not just flags.

function __swoof_complete
    set -l tokens (commandline -opc)
    set -l cur (commandline -ct)
    if test (count $tokens) -ge 2
        swoof __complete $tokens[2..-1] "$cur" 2>/dev/null
    else
        swoof __complete "$cur" 2>/dev/null
    end
end

complete -c swoof -f -a '(__swoof_complete)'
