import os
from pytrie import SortedStringTrie as Trie
import pydot
import re
import argparse

OB_CPP_INCLUDE = '^\s*#include\s*"storage/(.*)"\s'
OB_RE = re.compile(OB_CPP_INCLUDE)

OB_MODULES = {
    'transaction' : [
        'tx',
        'slog_ckpt',
        'concurrency_control',
        'tx_table',
        'tx_storage',
        'slog',
        'tablelock',
    ],
    'access' : [
        'access',
        'memtable',
        'ls',
        'compaction',
    ],
    'storage': [
        'tablet',
        'blockstore',
        'direct_load' ,
        'lob' ,
        'meta_mem' ,
        'mview' ,
        'tenant_snapshot',
        'vector_index',
        'backup',
        'checkpoint',
        'fts',
        'meta_store',
        'restore',
        'tmp_file',
        'blocksstable',
        'column_store',
        'ddl',
        'high_availability',
        'multi_data_source',
        'utl_file'
    ],
}

PG_MODULES = {
    'transaction': [
        'access/clog.h',
        'access/commit_ts.h',
        'access/multixact.h',
        'access/spgxlog.h',
        'access/subtrans.h',
        'access/twophase.h',
        'access/twophase_rmgr.h',
        'access/xact.h',
        'access/xlog.h',
        'access/xlog_internal.h',
        'access/xlogarchive.h',
        'access/xlogbackup.h',
        'access/xlogdefs.h',
        'access/xloginsert.h',
        'access/xlogprefetcher.h',
        'access/xlogreader.h',
        'access/xlogrecord.h',
        'access/xlogrecovery.h',
        'access/xlogstats.h',
        'access/xlogutils.h',
        'access/transam',
        'utils/time',
        'utils/snapshot.h'
        'utils/snapmgr.h'
    ],
    'access': [
        'access'
    ],
    'storage': [
        'storage'
    ]
}

PG_C_INCLUDE = '^\s*#include\s*"(.*)"\s'
PG_RE = re.compile(PG_C_INCLUDE)
PG_DIR = ['access', 'storage']

TIKV_MODULES = {
    "transaction": [
        'storage/mvcc',
        'storage/txn',
        'storage/lock_manager',
        'storage/raw',
        'server/lock_manager',
        'storage::mvcc',
        'storage::tx',
        'storage::raw',
        'storage::lock_manager',
        'server::lock_manager',
    ],
    "access": [

    ]
}
TIKV_USE = '^\s*use\s*(.*)\s*::\s*'
TIKV_RE = re.compile(TIKV_USE)
TIKV_DIR = []



TDDB_MODULES = {
    'CCB': [
        'concurrency'
    ],
    'RLB': [
        'replog',
        'raft'
    ],
    'DSB(access)': [
        'access',
        'store'
    ],
    'DSB(storage)': [
        'kv'
    ]
}
TDDB_C_INCLUDE = '^\s*#include\s*"(.*)"\s'
TDDB_RE = re.compile(TDDB_C_INCLUDE)
TDDB_DIR = ['src', 'include']

CONFIG = {
    "OB" : {"MODULES":OB_MODULES, "RE":OB_RE, "DIRS": set() },
    "PG" : {"MODULES":PG_MODULES, "RE":PG_RE, "DIRS": set(PG_DIR)},
    "TIKV": {"MODULES": TIKV_MODULES, "RE": TIKV_RE, "DIRS": set(TIKV_DIR)},
    "TDDB" : {"MODULES":TDDB_MODULES, "RE":TDDB_RE, "DIRS": set(), },
}

def walk_path(path, db):
    os.chdir(path)
    re_pattern = CONFIG[db]["RE"]
    modules = CONFIG[db]["MODULES"]
    filter_dirs = CONFIG[db]["DIRS"]
    path2mod = module_to_tire(modules)
    mod_dep = {}
    for root, dirs, files in os.walk(path, topdown=True):
        if len(filter_dirs) != 0 and root != path:
            relpath = os.path.relpath(root, path)
            ok = False;
            for d in filter_dirs:
                if relpath.startswith(d):
                    ok = True
                    break
            if not ok:
                continue

        for file in files:
            file_path = os.path.join(root, file)
            relpath = os.path.relpath(file_path, path)
            basename = os.path.basename(file_path)
            mod = file_mod(relpath, path2mod)
            if mod is None:
                continue
            print(file_path)
            if mod not in mod_dep:
                mod_dep[mod] = {}


            if basename.startswith('.'):
                continue


            include_files = read_file(file_path, re_pattern)
            for hf in include_files:
                supplier_mod = file_mod(hf, path2mod)
                if supplier_mod is None:
                    continue
                if supplier_mod in mod_dep[mod]:
                    mod_dep[mod][supplier_mod] = mod_dep[mod][supplier_mod] + 1
                else:
                    mod_dep[mod][supplier_mod] = 1
    print(mod_dep)
    gen_dot(mod_dep, db)

def gen_dot(mod_dep, name):
    mod_set = set()
    graph = pydot.Dot(name, graph_type="digraph")
    for (mod, supplier_mods) in mod_dep.items():
        if mod not in mod_set:
            node = pydot.Node(mod, label=mod)
            graph.add_node(node)
        for (supplier, weight) in supplier_mods.items():
            if supplier == mod:
                continue
            if supplier not in mod_set:
                node = pydot.Node(supplier, label=supplier)
                graph.add_node(node)
            edge = pydot.Edge(mod, supplier, label=weight)
            graph.add_edge(edge)
    print(graph.to_string())

def read_file(path, re_pattern):
    vec = []
    print("file ", path)
    with open(path, encoding='utf-8') as f:
        for line in f:
            m = re.search(re_pattern, line)
            if m is not None and len(m.groups()) > 0:
                include_file = m.groups()[0]
                vec.append(include_file)
    return vec



def file_mod(path,  path2mod:Trie):
    path = path.replace("\\", '/')
    try:
        (_, mod) = path2mod.longest_prefix_item(path)
        print("mod ", path,  mod)
        return mod
    except KeyError as _e:
        print ( "no such key: " + str(path))
        return None

def module_to_tire(module_dict):
    path2module = {}
    for module_name,path_list in module_dict.items():
        for p in path_list:
            path2module[p] = module_name

    return Trie(path2module)


def main():
    parser = argparse.ArgumentParser("mod_dep")
    parser.add_argument("-d", "--dir", help="dir to walk", type=str)
    parser.add_argument("-k", "--db-kind", help="DB kind", type=str)
    args = parser.parse_args()

    walk_path(args.dir, args.db_kind)

if __name__ == "__main__":
    main()
