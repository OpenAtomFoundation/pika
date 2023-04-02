#!/bin/bash
utils_dir=$(
    cd $(dirname $0)
    pwd
)
build_dir=$utils_dir/../build
results=""

function list_test_files() {
    local file_list=()
    local file
    for file in "$1"/*; do
        if [ -d "$file" ]; then
            file_list+=($(list_test_files "$file"))
        else
            local filename=$(basename "$file")
            if [[ "$filename" == *_test ]]; then
                file_list+=("$file")
            fi
        fi
    done
    echo "${file_list[@]}"
}

function run_test_file() {
    test_files=$(list_test_files "$build_dir")
    if [[ "$1" == "all" ]]; then
        for file in $test_files; do
            $file
            results="$results $?$file"
        done
    else
        for file in $test_files; do
            filename=$(basename "$file")
            if [[ "$1" == "$filename" ]]; then
                $file
                results="$results $?$file"
                echo $results
            fi
        done
    fi
}

run_test_file $1
echo "[All tests results]"
for result in $results; do
    filename=$(basename "$result")
    flag=${result:0:1}
    if [[ $flag == "0" ]]; then
        echo -e "\033[32m [ PASSED ] \033[0m\t$filename"
    elif [[ $flag == "1" ]]; then
        echo -e "\033[31m [ FAILED ] \033[0m\t$filename"
    fi
done
