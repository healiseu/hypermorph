

class HyperMorphError(Exception):
    """
    Base class for all HyperMorph-related errors
    """


class MISError(HyperMorphError):
    """
        Raised in operations with DataDictionary
    """


class AssociationError(HyperMorphError):
    """
    """


class ASETError(HyperMorphError):
    """
        Raised when it fails to construct an AssociativeSet instance
    """


class HACOLError(HyperMorphError):
    """
        Raised when it fails to initialize HACOL
    """


class WrongDictionaryType(HyperMorphError):
    """
    raised when we attempt to call a specific method on an object that has wrong node type
    """


class UnknownDictionaryType(HyperMorphError):
    """
    Raised when trying to add a term in the dictionary with an unknown type
    Types can be either :
    HyperEdges, i.e. instances of the TBoxTail class
    DRS, DMS, DLS - (dim4, 0   , 0)
    HLT, DS, DM   - (dim4, dim3, 0)
    HyperNodes, i.e. instances of the TBoxHead class
    TSV, CSV, FLD - (dim4, dim3, dim2)
    ENT, ATTR     - (dim4, dim3, dim2)
    """


class UnknownPrimitiveDataType(HyperMorphError):
    """
        Primitive Data Types are:
        ['bln', 'int', 'flt', 'date', 'time', 'dt', 'enm', 'uid', 'txt', 'wrd']
    """


class InvalidDelOperation(HyperMorphError):
    """
        Raised when you call DataManagementFramework.del() with invalid parameters
    """


class InvalidAddOperation(HyperMorphError):
    """
        Raised when you call DataManagementFramework.add() with invalid parameters
    """


class InvalidGetOperation(HyperMorphError):
    """
        Raised when you call DataManagementFramework.get() with invalid parameters
    """


class InvalidSQLOperation(HyperMorphError):
    """
        Raised when it fails to execute an SQL command
    """


class InvalidPipeOperation(HyperMorphError):
    """
        Raised when it fails to execute an operation in a pipeline
    """


class InvalidEngine(HyperMorphError):
    """
        Raised when we pass a wrong type of HyperMorph engine
    """


class InvalidSourceType(HyperMorphError):
    """
        Raised when we pass a wrong source type of HyperMorph
    """


class DBConnectionFailed(HyperMorphError):
    """
        Raised when it fails to create a connection with the database
    """


class PandasError(HyperMorphError):
    """
        Raised when it fails to construct pandas dataframe
    """


class ClickHouseException(HyperMorphError):
    """
        Raised when it fails to execute query in ClickHouse
    """


class GraphError(HyperMorphError):
    """
        Raised in Schema methods
    """


class GraphLinkError(HyperMorphError):
    """
        Raised in SchemaLink methods
    """


class GraphNodeError(HyperMorphError):
    """
        Raised in SchemaNode methods or in any of the methods of SchemaNode subclasses
    """

