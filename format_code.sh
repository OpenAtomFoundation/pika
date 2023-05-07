#!/bin/bash

find include src pika-tools -regex '.*\.\(cpp\|hpp\|c\|h\|cc\)' | xargs clang-format -i


# If you want to automatically format your code before git commit
# append the code to .git/hooks/pre-commit
#
# for FILE in $(git diff --cached --name-only | grep -E '.*\.(cpp|hpp|c|h|cc)')
# do
#    if [[ "$FILE" =~ .*.(cpp|hpp|c|h)$ ]];then
#      clang-format -i $FILE
#    fi
# done
