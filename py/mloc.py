#!/usr/bin/env python3
import argparse
import os
import re

DB_TYPE_SHARED = 'db-s'
DB_TYPE_SHARED_NOTHING = 'db-sn'
DB_TYPE_DETERMINISTIC = 'db-d'
DB_TYPE_TK = "db-tk"
OTHER = 'OTHER'
CCB = 'CCB'
DSB = 'DSB'
RLB = 'RLB'

common = 'common'
concurrency = 'concurrency'
debug = 'debug'
network = 'network'
portal = 'portal'
proto = 'proto'
raft = 'raft'
replog = 'replog'
store = 'store'

Module2Block = {
    common: OTHER,
    concurrency: CCB,
    debug: OTHER,
    network: OTHER,
    portal: OTHER,
    proto: OTHER,
    raft: RLB,
    replog: RLB,
    store: DSB,
}

DEF_DB_TYPE_SHARED = "DB_TYPE_SHARED"
DEF_DB_TYPE_SHARE_NOTHING = "DB_TYPE_SHARE_NOTHING"
DEF_DB_TYPE_CALVIN = "DB_TYPE_CALVIN"
DEF_DB_TYPE_GEO_REP_OPTIMIZE = "DB_TYPE_GEO_REP_OPTIMIZE"
DEF_DB_TYPE_TK = "DB_TYPE_TK"
DEF_DB_TYPE_ROCKS = "DB_TYPE_ROCKS"
DEF_DB_TYPE_NON_DETERMINISTIC = "DB_TYPE_NON_DETERMINISTIC"

DBTypeMacro = {
    DEF_DB_TYPE_SHARED,
    DEF_DB_TYPE_SHARE_NOTHING,
    DEF_DB_TYPE_CALVIN,
    DEF_DB_TYPE_GEO_REP_OPTIMIZE,
    DEF_DB_TYPE_TK,
    DEF_DB_TYPE_ROCKS,
    DEF_DB_TYPE_NON_DETERMINISTIC,
}

DB_TYPES = [
    DB_TYPE_SHARED,
    DB_TYPE_SHARED_NOTHING,
    DB_TYPE_DETERMINISTIC,
    DB_TYPE_DETERMINISTIC,
    DB_TYPE_TK
]

Type2Macro = {
    DB_TYPE_SHARED: {
        DEF_DB_TYPE_SHARED,
        DEF_DB_TYPE_NON_DETERMINISTIC,
        DEF_DB_TYPE_ROCKS
    },
    DB_TYPE_SHARED_NOTHING: {
        DEF_DB_TYPE_SHARE_NOTHING,
        DEF_DB_TYPE_NON_DETERMINISTIC,
        DEF_DB_TYPE_ROCKS
    },
    DB_TYPE_DETERMINISTIC: {
        DEF_DB_TYPE_CALVIN,
        DEF_DB_TYPE_ROCKS
    },
    DB_TYPE_TK: {
        DEF_DB_TYPE_SHARE_NOTHING,
        DEF_DB_TYPE_NON_DETERMINISTIC,
        DEF_DB_TYPE_TK
    },
}

DEF_DB_SHARED = 'DB_SHARED'
DEF_DB_SHARE_NOTHING = ''


def run_shell_command(command):
    lines = ""
    proc = os.popen(command)
    for line in proc.readlines():
        line.rstrip()
        if line != '':
            # print(line)
            lines += line + "\n"
    proc.close()
    return lines


def code_unifdef(in_dir, out_dir, db_type):
    if in_dir == out_dir:
        return
    proj_name = os.path.basename(in_dir)

    source_files = {CCB: [], DSB: [], RLB: [], OTHER: []}

    for in_sub_dir in ['include', 'src', 'test']:
        for root, dirs, files in os.walk(os.path.join(in_dir, in_sub_dir)):
            for f in files:
                if re.match(r'^.*\.((hpp)|(h)|(cpp)|(hh))$', f):
                    source_code = os.path.join(root, f)
                    base_root = os.path.basename(root)
                    if base_root in Module2Block:
                        block = Module2Block[base_root]
                        source_files[block].append(source_code)

    tar_path_map = {}
    for block in source_files:
        out_proj_dir = os.path.join(out_dir, block)
        out_proj_dir = os.path.join(out_proj_dir, proj_name)
        if in_dir == out_proj_dir:
            return

        defined = ''
        for macro in Type2Macro[db_type]:
            defined = defined + ' -iD{} '.format(macro)
        for macro in DBTypeMacro:
            if macro not in Type2Macro[db_type]:
                defined = defined + ' -iU{}'.format(macro)

        for src in source_files[block]:
            relpath = os.path.relpath(src, in_dir)
            output_source = os.path.join(out_proj_dir, relpath)
            out_relpath = os.path.dirname(output_source)
            os.makedirs(out_relpath, mode=0o777, exist_ok=True)

            command = 'unifdef {} {} -o {}'.format(src, defined, output_source)
            # print(command)

            run_shell_command(command)

        tar_name = proj_name + '.' + db_type + '.' + block + '.tar'
        tar_path = os.path.join(out_dir, tar_name)
        command = 'tar -C {} -cvf {} {}'.format(out_dir, tar_path, out_proj_dir)
        # print(command)
        run_shell_command(command)
        tar_path_map[block] = tar_path

    return tar_path_map


re_cloc_output_sum = r"SUM:\s*" \
                     r"same(\s+\d+)(\s+\d+)(\s+\d+)(\s+\d+)\s*" \
                     r"modified(\s+\d+)(\s+\d+)(\s+\d+)(\s+\d+)\s*" \
                     r"added(\s+\d+)(\s+\d+)(\s+\d+)(\s+\d+)\s*" \
                     r"removed(\s+\d+)(\s+\d+)(\s+\d+)(\s+\d+)\s*"


def diff_block_loc(path, dbt2path, db_t1, db_t2):
    t1_tar_path = dbt2path[db_t1]
    t2_tar_path = dbt2path[db_t2]
    # print(t1_tar_path)
    # print(t2_tar_path)
    for b in t1_tar_path:
        p1 = t1_tar_path[b]
        p2 = t2_tar_path[b]
        print('diff LOC between {} and {}, block : {}'.format(db_t1, db_t2, b))
        command = 'cloc --diff {} {}'.format(p1, p2)
        output = run_shell_command(command)
        match = re.search(re_cloc_output_sum, output)
        print('same      ', match.group(4))
        print('modified  ', match.group(8))
        print('added     ', match.group(12))
        print('removed   ', match.group(16))
        print('------------------------------------')
        print('')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='mloc')
    project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dbt2path = {}
    for dbt in DB_TYPES:
        p = code_unifdef(project_path, "/tmp/block-db/" + dbt, dbt)
        dbt2path[dbt] = p

    print("from shared to shared-nothing DB ...")
    diff_block_loc(project_path, dbt2path, DB_TYPE_SHARED, DB_TYPE_SHARED_NOTHING)
    print("from shared-nothing to deterministic DB ...")
    diff_block_loc(project_path, dbt2path, DB_TYPE_SHARED_NOTHING, DB_TYPE_DETERMINISTIC)
    print("from shared-nothing to TK DB ...")
    diff_block_loc(project_path, dbt2path, DB_TYPE_SHARED_NOTHING, DB_TYPE_TK)
