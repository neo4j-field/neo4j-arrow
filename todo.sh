#!/bin/sh
find . -name '*.java' \
    | xargs grep TODO \
    | awk -F'//' '{ gsub(/[ :]/, "", $1); printf "- [ ]%s [%s](%s)\n", $2, substr($1, match($1, /\/\w+[\\.]java/) + 1), $1 }' \
    | grep -v '<p>'