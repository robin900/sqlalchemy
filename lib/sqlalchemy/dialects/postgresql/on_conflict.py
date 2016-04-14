from ...sql.expression import ClauseElement, ColumnClause, ColumnElement
from ...sql.elements import _literal_as_text, and_
from ...util import string_types, OrderedDict

__all__ = ('DoUpdate', 'DoNothing')

_UPDATE_SET_EXCLUDED = '__update_set_excluded__'

CONFLICT_TARGET_COLUMN_TYPES = tuple(string_types) + (ColumnClause,)

def resolve_possibly_named_object_to_str(col):
    if isinstance(col, string_types):
        return col
    else:
        return getattr(col, 'name', None)

class OnConflictClause(ClauseElement):
    def __init__(self):
        super(OnConflictClause, self).__init__()
        self.action_do = None
        self.conflict_target_type = None
        self.conflict_target_elements = None
        self.conflict_target_whereclause = None

    def on_constraint(self, constraint):
        """
        :param constraint:
           A string naming the unique or exclusion constraint to use
           as the conflict target, or the constraint object itself.
        """
        constraint_name = resolve_possibly_named_object_to_str(constraint)
        if constraint_name is None:
            raise ValueError("constraint_name must be non-empty string or named Constraint object")
        self.conflict_target_type = 'constraint'
        self.conflict_target_elements = constraint_name
        return self

    def on_columns(self, *columns, **kw):
        """
        :param \*columns:
        Each element can be a string naming a column, or the column object itself. 
        At least one is required.

        :param where:
        Optional, keyword-only argument. If present, can be a literal SQL string
        or an acceptable expression for the WHERE clause in the conflict target
        expression; this can be useful to locate a partial index with the same
        WHERE clause to use as the index to provide conflict detection.
        """
        if not columns:
            raise ValueError("at least one column argument is required")
        self.conflict_target_type = 'columns'
        # TODO each element has three components: an expression; an optional collate; an optional opclass
        extracted_columns = []
        for col in columns:
            colexpr = resolve_possibly_named_object_to_str(col)
            if not colexpr:
                raise ValueError("Column arguments must be name strings, or Column objects, not %r" % col)
            extracted_columns.append(colexpr)
        self.conflict_target_elements = extracted_columns
        where = kw.get('where')
        if where is not None:
            self.conflict_target_whereclause = _literal_as_text(where)
        return self

class DoUpdate(OnConflictClause):
    """
    Represents an ``ON CONFLICT`` clause with a  ``DO UPDATE SET ...`` action.
    """
    def __init__(self):
        super(DoUpdate, self).__init__()
        self.action_do = 'update'
        self.update_values_to_set = OrderedDict()
        self.update_whereclause = None

    def set_with_excluded(self, *columns):
        """
        :param \*columns:
          One or more :class:`.Column` objects or strings representing column names.
          These columns will be added to the ``SET`` clause using the `excluded` row's
          values from the same columns. e.g. ``SET colname = excluded.colname``.
        """
        for col in columns:
            colname = resolve_possibly_named_object_to_str(col)
            if not colname:
                raise ValueError("Columns to set with excluded values must be strings or have a .name")
            self.update_values_to_set[colname] = _UPDATE_SET_EXCLUDED
        return self

    def where(self, whereclause):
        """
        :param whereclause:
        Can be a literal SQL string or an acceptable expression to be the
        WHERE clause for the DO UPDATE action, further filtering which
        rows will be updated when a conflict is detected.
        """
        if self.update_whereclause is not None:
            self.update_whereclause = and_(self.update_whereclause, _literal_as_text(whereclause))
        else:
            self.update_whereclause = _literal_as_text(whereclause)
        return self

class DoNothing(OnConflictClause):
    """
    Represents an ``ON CONFLICT`` clause with a ``DO NOTHING`` action.
    """
    def __init__(self):
        super(DoNothing, self).__init__()
        self.action_do = 'nothing'
