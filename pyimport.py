import pyarrow as pa
import pyarrow.csv as csv

from os import curdir, listdir
import os.path as ospath

_DIR = ospath.abspath(curdir)

_NODE_ID = 'ID'
_START_ID = 'START_ID'
_END_ID = 'END_ID'

def load_import_csv(prefix, basedir=_DIR, extra_cols={}):
    files = [
            f for f in listdir(basedir)
            if f.startswith(prefix) and not 'header' in f
    ]
    fields, all_fields = [], []
    is_nodes = None
    # Look at the header file to determine schema and data type (node vs rels)
    with open(ospath.join(basedir, f'{prefix}header.csv')) as header:
        for col in header.readline().rstrip().split(','):
            name, _type = col.split(':')
            if _type == _START_ID or _type == _END_ID:
                is_nodes = False
                all_fields.append(_type)
                fields.append(_type)
            elif _type == _NODE_ID:
                is_nodes = True
                all_fields.append(_type)
                fields.append(_type)
            elif '[]' in _type:
                # we don't handle arrays yet in PyArrow :(
                all_fields.append(name)
            else:
                all_fields.append(name)
                fields.append(name)

        if is_nodes is None:
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
            if is_nodes:
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

    return pa.concat_tables(tables)

