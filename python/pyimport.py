import pyarrow as pa
import pyarrow.csv as csv

from enum import Enum
from os import curdir, listdir
import os.path as ospath

_DIR = ospath.abspath(curdir)

_NODE_ID = 'ID'
_START_ID = 'START_ID'
_END_ID = 'END_ID'

class EntityType(Enum):
    UNKNOWN = 0
    NODE = 1
    REL = 2

def load_dir(path):
    """
    Import all CSV's in a given directory path.

    Returns a tuple of lists of node tables and lists of relationship tables.
    """
    root = ospath.expanduser(path)
    print(f'Loading from {root}')
    
    targets = set()
    for f in listdir(root):
        # assumption is we have files like: '(nodes|relationships)_(type?)_N.csv'
        # we want to find a representation for the unique entities
        front_part = '_'.join(f.split('_')[0:-1]) + '_'
        targets.add(front_part)

    print(f'import targets: {targets}')
    nodes, rels = [], []
    for target in targets:
        # we may have a missing label...use a generic label
        extra = {}
        if target.startswith('node') and target.count('_') == 1:
            extra = { '_labels_': [['Node']] }
        table, entity_type = load_import_csv(target, basedir=path, extra_cols=extra)
        if entity_type == EntityType.NODE:
            nodes.append(table)
        elif entity_type == EntityType.REL:
            rels.append(table)
        else:
            print(f'bad import of target {target}')

    # build final master tables. need promotion to NULL or NAN for sparse properties.
    node_table = pa.concat_tables(nodes, promote=True)
    rels_table = pa.concat_tables(rels, promote=True)

    return node_table, rels_table


def load_import_csv(prefix, basedir=_DIR, extra_cols={}):
    """
    Load all import CSV files with the same 'prefix' (e.g. 'nodes_USER_') from
    the given basedir path.

    Additional constant values can be added via the extra_cols allowing for
    adding things like node labels when such data is missing from the import.
    
    For example: 
        - To add node labels: { '_labels_': ['User'] }
        - To add relationship types: { '_type_': 'FOLLOWS' }
        - To add a general static value: { 'my_property': 3.14 }

    Returns a tuple of:
        - new PyArrow Table from the CSV data files
        - EntityType for the table (e.g. Node vs Rel)
    """
    files = [
            f for f in listdir(basedir)
            if f.startswith(prefix) and not 'header' in f
    ]
    fields, all_fields = [], []
    entity_type = EntityType.UNKNOWN

    # Look at the header file to determine schema and data type (node vs rels)
    with open(ospath.join(basedir, f'{prefix}header.csv')) as header:
        for col in header.readline().rstrip().split(','):
            name, _type = col.split(':')
            if _type == _START_ID or _type == _END_ID:
                entity_type = EntityType.REL
                all_fields.append(_type)
                fields.append(_type)
            elif _type == _NODE_ID:
                entity_type = EntityType.NODE
                all_fields.append(_type)
                fields.append(_type)
            elif '[]' in _type:
                # we don't handle arrays yet in PyArrow :(
                all_fields.append(name)
            else:
                all_fields.append(name)
                fields.append(name)

        if entity_type == EntityType.UNKNOWN:
            print(f'!!! missing key schema fields: {[_NODE_ID] + list(_REL_TYPES)}')
            return None

    print(f'files = {files}\nfields = {fields}\n')

    tables = []
    for _file in files:
        path = ospath.join(basedir, _file)
        print(f'reading {path}...')
        read_opts = csv.ReadOptions(column_names=all_fields)
        convert_opts = csv.ConvertOptions(include_columns=fields)
        stream = csv.open_csv(path, read_options=read_opts, convert_options=convert_opts)
        table = stream.read_all()

        # see if we can discern a label or type
        parts = _file.split('_')
        if len(parts) > 2:
            name = ''
            value = '_'.join(parts[1:-1])
            if entity_type == EntityType.NODE:
                name = '_labels_'
                value = [value]
            else:
                name = '_type_'
            col = pa.array([value] * len(table))
            table = table.append_column(name, col)

        # gds csv export doesn't make labels if we use random generator :(
        for col in extra_cols:
            extra = pa.array(extra_cols[col] * len(table))
            table = table.append_column(col, extra)
        tables.append(table)

    return pa.concat_tables(tables), entity_type

