import pyarrow as pa
import pyarrow.csv
from pyarrow.lib import Array, Table

from enum import Enum
from typing import cast, Any, Dict, List, NamedTuple, Tuple, Union
from os import curdir, listdir
import os.path as ospath

# Default globals
_DIR = ospath.abspath(curdir)
_GLOBAL_ID = 'Global'

# Types, etc.
class EntityType(Enum): ...
class EntityType(Enum):
    UNKNOWN = 'UNKNOWN'
    NODE = 'NODE'
    REL = 'REL'

    @classmethod
    def from_str(cls, s: str) -> EntityType:
        for _type in EntityType:
            if _type.value == s:
                return _type
        return EntityType.UNKNOWN

class FieldType(Enum): ...
class FieldType(Enum):
    """
    Type of importable field. See the ops manual:
    https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/#import-tool-header-format-properties

    XXX: Not all types supported.
    """
    NODE_ID = 'ID'
    START_ID = 'START_ID'
    END_ID = 'END_ID'
    STRING = 'string'
    SHORT = 'short'
    INT = 'int'
    LONG = 'long'
    FLOAT = 'float'
    DOUBLE = 'double'
    BOOLEAN = 'boolean'
    BYTE = 'byte'

    @classmethod
    def from_str(cls, s: str) -> FieldType:
        for _type in FieldType:
            if _type.value == s:
                return _type
        return FieldType.STRING

class Field(NamedTuple):
    name: str
    type: FieldType = FieldType.STRING
    id_space: str = _GLOBAL_ID

class Entity(NamedTuple):
    type: EntityType = EntityType.UNKNOWN
    fields: List[Field] = []
    files: List[str] = []

def _include_cols(fields: List[Field]) -> List[str]:
    """
    Identify which columns to include in CSV parsing. Unmatched columns will
    be ignored.
    :param fields: list of Fields
    :return: list of field names as strings
    """
    # TODO
    return [f.name for f in fields]

def _parse_field(field: str) -> Field:
    """
    Parse the given string representation of a CSV import field.

    :param field: string or string-like field input
    :return: a new Field
    """
    name, _type = str(field).split(':')
    if '(' in _type and _type.endswith(')'):
        _type, id_space = _type.split('(')[0:-1]
        return Field(name, FieldType.from_str(_type), id_space)
    return Field(name, FieldType.from_str(_type))


def _parse_header(header: str, delimiter: str = ',') -> List[Field]:
    """
    Parse a bulk-import header, pulling out field names, types, and id spaces.

    Example header:
        personId:ID(Person),age:int,active:boolean,name,vector:float[]
    :param header: string or string-like file header to parse
    :param delimiter: column delimiter (default ',')
    :return: list of parsed Fields
    """
    parts = str(header).split(delimiter)
    return [_parse_field(f) for f in parts]


def load_dir(path: str, delimiter: str =',') -> Tuple[Any, Any]:
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
        table, entity = load_import_csv(target, basedir=path,
                                             delimiter=delimiter,
                                             extra_cols=extra)
        if entity.type == EntityType.NODE:
            nodes.append(table)
        elif entity.type == EntityType.REL:
            rels.append(table)
        else:
            print(f'bad import of target {target}')

    # Build final master tables. Need promotion to NULL or NAN for sparse
    # properties.
    # TODO: promote=True results in data copy, but use this shortcut for now
    node_table = pa.concat_tables(nodes, promote=True)
    rels_table = pa.concat_tables(rels, promote=True)

    return node_table, rels_table


def load_import_csv(prefix: str, basedir: str = _DIR, delimiter: str = ',',
                    extra_cols: Dict[Any, Any] = {}) -> Tuple[Table, Entity]:
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
        - Entity for the table (e.g. Node vs Rel)
    """
    files = [
            f for f in listdir(basedir)
            if f.startswith(prefix) and not 'header' in f
    ]
    # Look at the header file to determine schema and data type (node vs rels)
    fields = []
    with open(ospath.join(basedir, f'{prefix}header.csv')) as header_file:
        line = header_file.readline().strip()
        fields = _parse_header(line, delimiter=delimiter)
    print(f'files = {files}\nfields = {fields}\n')

    entity_type = None
    for field in fields:
        if field.type == FieldType.NODE_ID:
            entity_type = EntityType.NODE
            break
        elif field.type in [FieldType.START_ID, FieldType.END_ID]:
            entity_type = EntityType.REL
            break
    entity = Entity(entity_type or EntityType.UNKNOWN, fields, files)

    tables = []
    for filename in entity.files:
        path = ospath.join(basedir, filename)
        print(f'reading {path}...')
        read_opts = pa.csv.ReadOptions(
            column_names=[f.name for f in entity.fields])
        convert_opts = pa.csv.ConvertOptions(
            include_columns=_include_cols(entity.fields))
        table = pa.csv.read_csv(path, read_options=read_opts,
                             convert_options=convert_opts)

        # See if we can discern a label or type
        # TODO: split this out into special handling logic as the labels or
        #       types might come from different places
        value: Union[str, List[str]]
        parts = filename.split('_')
        if len(parts) > 2:
            value = '_'.join(parts[1:-1])
            if entity.type == EntityType.NODE:
                name = '_labels_'
                value = [value]
            else:
                name = '_type_'
            table = table.append_column(name, pa.array([value] * len(table)))

        # GDS csv export doesn't make labels if we use random generator :(
        for col in extra_cols:
            extra = pa.array(extra_cols[col] * len(table))
            table = table.append_column(str(col), extra)
        tables.append(table)

    return pa.concat_tables(tables), entity

