#!/usr/bin/env python3
#
#===- run-clang-tidy.py - Parallel clang-tidy runner ---------*- python -*--===#
#
#                     The LLVM Compiler Infrastructure
#
#===------------------------------------------------------------------------===#
# FIXME: Integrate with clang-tidy-diff.py

"""
Parallel clang-tidy runner
==========================
Runs clang-tidy over all files in a compilation database. Requires clang-tidy
and clang-apply-replacements in $PATH.
Example invocations.
- Run clang-tidy on all files in the current working directory with a default
  set of checks and show warnings in the cpp files and all project headers.
    run-clang-tidy.py $PWD
- Fix all header guards.
    run-clang-tidy.py -fix -checks=-*,llvm-header-guard
- Fix all header guards included from clang-tidy and header guards
  for clang-tidy headers.
    run-clang-tidy.py -fix -checks=-*,llvm-header-guard extra/clang-tidy \
                      -header-filter=extra/clang-tidy
Compilation database setup:
http://clang.llvm.org/docs/HowToSetupToolingForLLVM.html
"""

from __future__ import division
from __future__ import print_function

import argparse
import glob
import json
import multiprocessing
import os
import pprint
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import traceback
# import yaml


is_py2 = sys.version[0] == '2'

if is_py2:
    import Queue as queue
else:
    import queue as queue


def find_compilation_database(path):
    """Adjusts the directory until a compilation database is found."""
    result = './'
    while not os.path.isfile(os.path.join(result, path)):
        if os.path.realpath(result) == '/':
            print('Error: could not find compilation database.')
            sys.exit(1)
        result += '../'
    return os.path.realpath(result)


def make_absolute(f, directory):
    if os.path.isabs(f):
        return f
    return os.path.normpath(os.path.join(directory, f))


def get_tidy_invocation(f, clang_tidy_binary, checks, tmpdir, build_path,
                        header_filter, extra_arg, extra_arg_before, quiet,
                        config):
    """Gets a command line for clang-tidy."""
    start = [clang_tidy_binary]
    if header_filter is not None:
        start.append('-header-filter=' + header_filter)
    else:
        pass
    if checks:
        start.append('-checks=' + checks)
    if tmpdir is not None:
        start.append('-export-fixes')
        # Get a temporary file. We immediately close the handle so clang-tidy can
        # overwrite it.
        (handle, name) = tempfile.mkstemp(suffix='.yaml', dir=tmpdir)
        os.close(handle)
        start.append(name)
    for arg in extra_arg:
        start.append('-extra-arg=%s' % arg)
    for arg in extra_arg_before:
        start.append('-extra-arg-before=%s' % arg)
    start.append('-p=' + build_path)
    if quiet:
        start.append('-quiet')
    if config:
        start.append('-config=' + config)
    start.append(f)
    return start


def merge_replacement_files(tmpdir, mergefile):
    """Merge all replacement files in a directory into a single file"""
    # The fixes suggested by clang-tidy >= 4.0.0 are given under
    # the top level key 'Diagnostics' in the output yaml files
    mergekey = "Diagnostics"
    merged = []
    for replacefile in glob.iglob(os.path.join(tmpdir, '*.yaml')):
        content = yaml.safe_load(open(replacefile, 'r'))
        if not content:
            continue  # Skip empty files.
        merged.extend(content.get(mergekey, []))

    if merged:
        # MainSourceFile: The key is required by the definition inside
        # include/clang/Tooling/ReplacementsYaml.h, but the value
        # is actually never used inside clang-apply-replacements,
        # so we set it to '' here.
        output = {'MainSourceFile': '', mergekey: merged}
        with open(mergefile, 'w') as out:
            yaml.safe_dump(output, out)
    else:
        # Empty the file:
        open(mergefile, 'w').close()


def check_clang_apply_replacements_binary(args):
    """Checks if invoking supplied clang-apply-replacements binary works."""
    try:
        subprocess.check_call(
            [args.clang_apply_replacements_binary, '--version'])
    except:
        print('Unable to run clang-apply-replacements. Is clang-apply-replacements '
              'binary correctly specified?', file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


def apply_fixes(args, tmpdir):
    """Calls clang-apply-fixes on a given directory."""
    invocation = [args.clang_apply_replacements_binary]
    if args.format:
        invocation.append('-format')
    if args.style:
        invocation.append('-style=' + args.style)
    print(tmpdir)
    invocation.append(tmpdir)
    subprocess.call(invocation)

def remove_pb_files(filenames):
    for file in filenames.copy():
        if file.endswith('.pb.cc'):
            filenames.remove(file)

def remove_test_files(filenames):
    for file in filenames.copy():
        if file.endswith('_test.cc'):
            filenames.remove(file)

# TODO(LeeHao): add some compile failed file ,eg src/storage/benchmark
def remove_custom_files(filenames):
    for file in filenames.copy():
        # if file.endswith('pika_pubsub.cc') or file.endswith('pika_stable_log.cc') or file.endswith('pstd_string.cc') or file.endswith('pika_zset.cc') or file.endswith('pika_server.cc'):
        #     filenames.remove(file)
        if 'benchmark' in os.path.dirname(file) or 'examples' in os.path.dirname(file) or 'performance' in os.path.dirname(file):
            filenames.remove(file)


def run_tidy(args, tmpdir, build_path, queue, lock, failed_files):
    """Takes filenames out of queue and runs clang-tidy on them."""
    while True:
        name = queue.get()
        print("Checking: {}".format(name))
        sys.stdout.flush()
        invocation = get_tidy_invocation(name, args.clang_tidy_binary, args.checks,
                                         tmpdir, build_path, args.header_filter,
                                         args.extra_arg, args.extra_arg_before,
                                         args.quiet, args.config)

        proc = subprocess.Popen(
            invocation, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, err = proc.communicate()
        if proc.returncode != 0:
            failed_files.append(name)
        with lock:
            output = output.decode('utf-8') if output is not None else None
            err = err.decode('utf-8') if output is not None else None
            # unfortunately, our error messages are actually on STDOUT
            # STDERR tells how many warnings are generated,
            # but this includes non-user-code warnings, so it is useless...
            if output:
                sys.stdout.write('\n')
                sys.stdout.write(output)
        queue.task_done()


def main():
    parser = argparse.ArgumentParser(description='Runs clang-tidy over all files '
                                                 'in a compilation database. Requires '
                                                 'clang-tidy and clang-apply-replacements in '
                                                 '$PATH.')
    parser.add_argument('-clang-tidy-binary', metavar='PATH',
                        default='clang-tidy',
                        help='path to clang-tidy binary')
    parser.add_argument('-clang-apply-replacements-binary', metavar='PATH',
                        default='clang-apply-replacements',
                        help='path to clang-apply-replacements binary')
    parser.add_argument('-checks', default=None,
                        help='checks filter, when not specified, use clang-tidy '
                             'default')
    parser.add_argument('-config', default=None,
                        help='Specifies a configuration in YAML/JSON format: '
                             '  -config="{Checks: \'*\', '
                             '                       CheckOptions: [{key: x, '
                             '                                       value: y}]}" '
                             'When the value is empty, clang-tidy will '
                             'attempt to find a file named .clang-tidy for '
                             'each source file in its parent directories.')
    parser.add_argument('-header-filter', default=None,
                        help='regular expression matching the names of the '
                             'headers to output diagnostics from. Diagnostics from '
                             'the main file of each translation unit are always '
                             'displayed.')
    parser.add_argument('-export-fixes', metavar='filename', dest='export_fixes',
                        help='Create a yaml file to store suggested fixes in, '
                             'which can be applied with clang-apply-replacements.')
    parser.add_argument('-j', type=int, default=0,
                        help='number of tidy instances to be run in parallel.')
    parser.add_argument('files', nargs='*', default=['.*'],
                        help='files to be processed (regex on path)')
    parser.add_argument('-fix', action='store_true', help='apply fix-its')
    parser.add_argument('-format', action='store_true', help='Reformat code '
                                                             'after applying fixes')
    parser.add_argument('-style', default='file', help='The style of reformat '
                                                       'code after applying fixes')
    parser.add_argument('-p', dest='build_path',
                        help='Path used to read a compile command database.')
    parser.add_argument('-extra-arg', dest='extra_arg',
                        action='append', default=[],
                        help='Additional argument to append to the compiler '
                             'command line.')
    parser.add_argument('-extra-arg-before', dest='extra_arg_before',
                        action='append', default=[],
                        help='Additional argument to prepend to the compiler '
                             'command line.')
    parser.add_argument('-quiet', action='store_true',
                        help='Run clang-tidy in quiet mode')
    parser.add_argument('-only-diff', action='store_true',
                        help='Only run clang-tidy on diff file to master branch')
    args = parser.parse_args()

    db_path = 'compile_commands.json'

    if args.build_path is not None:
        build_path = args.build_path
    else:
        # Find our database
        build_path = find_compilation_database(db_path)

    try:
        invocation = [args.clang_tidy_binary, '-list-checks']
        invocation.append('-p=' + build_path)
        if args.checks:
            invocation.append('-checks=' + args.checks)
        invocation.append('-')
        subprocess.check_call(invocation)
    except:
        print("Unable to run clang-tidy.", file=sys.stderr)
        sys.exit(1)

    # Load the database and extract all files.
    database = json.load(open(os.path.join(build_path, db_path)))
    files = [make_absolute(entry['file'], entry['directory'])
             for entry in database]

    remove_pb_files(files)
    remove_test_files(files)
    remove_custom_files(files)

    # Running clang-tidy in the whole project is slow. Therefore, we added
    # support for running clang-tidy on git diff. When `only_diff` is
    # specified in the command line, we only check files modified compared
    # with origin/master, so as to speed up clang-tidy check.
    #
    # This functionality is set as CMake target `check-clang-tidy-diff`.
    # You can use `make check-clang-tidy-diff` to do a fast clang-tidy
    # check.
    if args.only_diff:
        git_repo = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"], capture_output=True)
        git_repo_path = git_repo.stdout.decode("utf-8").strip()
        # Get all files changed compared with origin/master
        result = subprocess.run(
            ["git", "--no-pager", "diff", "--name-only", "origin/master"], capture_output=True)
        git_changed_file_list = list(map(lambda x: make_absolute(
            x, git_repo_path), result.stdout.decode("utf-8").strip().split("\n")))
        git_changed_file_set = set(git_changed_file_list)
        # Only retain files that exists in git diff
        files = list(filter(lambda x: x in git_changed_file_set, files))

    max_task = args.j
    if max_task == 0:
        max_task = multiprocessing.cpu_count()

    tmpdir = None
    if args.fix or args.export_fixes:
        check_clang_apply_replacements_binary(args)
        tmpdir = tempfile.mkdtemp()

    # Build up a big regexy filter from all command line arguments.
    file_name_re = re.compile('|'.join(args.files))

    return_code = 0
    try:
        # Spin up a bunch of tidy-launching threads.
        task_queue = queue.Queue(max_task)
        # List of files with a non-zero return code.
        failed_files = []
        lock = threading.Lock()
        for _ in range(max_task):
            t = threading.Thread(target=run_tidy,
                                 args=(args, tmpdir, build_path, task_queue, lock, failed_files))
            t.daemon = True
            t.start()

        def update_progress(current_file, num_files):
            pct = int(current_file / num_files * 100)
            if current_file == num_files or pct % max(2, num_files // 10) == 0:
                stars = pct // 10
                spaces = 10 - pct // 10
                print('\rProgress: [{}{}] ({}% / File {} of {})'.format(
                    'x' * stars,
                    ' ' * spaces,
                    pct,
                    current_file,
                    num_files
                ), end='')
                sys.stdout.flush()
                if current_file == num_files:
                    print()

        # Fill the queue with files.
        for i, name in enumerate(files):
            if file_name_re.search(name):
                put_file = False
                while not put_file:
                    try:
                        task_queue.put(name, block=True, timeout=300)
                        put_file = True
                        # update_progress(i, len(files))
                    except queue.Full:
                        print('Still waiting to put files into clang-tidy queue.')
                        sys.stdout.flush()

        # Wait for all threads to be done.
        task_queue.join()
        # update_progress(100, 100)
        if len(failed_files):
            return_code = 1
            print('The files that failed were:')
            print(pprint.pformat(failed_files))
            print(
                'Note that a failing .h file will fail all the .cpp files that include it.\n')

    except KeyboardInterrupt:
        # This is a sad hack. Unfortunately subprocess goes
        # bonkers with ctrl-c and we start forking merrily.
        print('\nCtrl-C detected, goodbye.')
        if tmpdir:
            shutil.rmtree(tmpdir)
        os.kill(0, 9)

    if args.export_fixes:
        print('Writing fixes to ' + args.export_fixes + ' ...')
        try:
            merge_replacement_files(tmpdir, args.export_fixes)
        except:
            print('Error exporting fixes.\n', file=sys.stderr)
            traceback.print_exc()
            return_code = 1

    if args.fix:
        print('Applying fixes ...')
        try:
            apply_fixes(args, tmpdir)
        except:
            print('Error applying fixes.\n', file=sys.stderr)
            traceback.print_exc()
            return_code = 1

    if tmpdir:
        shutil.rmtree(tmpdir)
    print("")
    sys.stdout.flush()
    sys.exit(return_code)


if __name__ == '__main__':
    main()
