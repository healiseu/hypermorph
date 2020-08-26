

import collections
from hypermorph.exceptions import AssociationError


class Association(object):
    """
    This is the analogue of a relational tuple, i.e. row of ordered values
    An Association is the basic construct of Associative Sets

    It is called Association because it associates a HyperBond to a set of HyperAtoms
    HyperBond is a symbolic 2D numerical representation of a row and
    HyperAtom is a symbolic 2D numerical representation of a unique value in the table column
    HyperAtoms can also have textual (string) representation

    Association can be represented in many ways:
    i) With the hb key
    A[7, 4]

    ii) With keyword arguments
    Association(hb=(7, 4), prtcol=None, prtwgt=None, prtID=227, prtnam='car battery', prtunt=None)

    iii) With positional arguments
    Association((7,4), None, None, 227, 'car battery', None)

    Association in Python is implemented as a named tuple with a heading and a body
    --------------------------------------------------------------------------------
    heading: a set of attributes and a key  e.g.
    ('hb', 'prtcol', 'prtwgt', 'prtID', 'prtnam', 'prtunt')

    body: KV pairs e.g.
    Association(hb=(7, 4), prtcol=None, prtwgt=None, prtID=227, prtnam='car battery', prtunt=None)
    """
    @staticmethod
    def change_heading(*fields):
        defaults = (None, )*len(fields)
        Association._assoc_factory = collections.namedtuple('Association', fields, defaults=defaults)
        Association._assoc_fields = fields

    # Class variables, i.e accessed by all instances of Association class
    _assoc_factory = None
    _assoc_fields = None

    def __init__(self, *pos_args, **kw_args):
        if kw_args:
            pass
        #   self._hb = kw_args.get('hb')
        elif pos_args and self.heading_fields:
            kw_args = dict(zip(self.heading_fields, pos_args))
            # self._hb = kw_args.get('hb')
        else:
            raise AssociationError('Failed to construct Association from positional or keyword arguments')

        # if self._hb is None:
        #     raise AssociationError('Failed to construct Association, `hb` is a mandatory field in the heading')

        if not Association._assoc_factory:
            # This is executed when the first created instance of Association is initialized
            # it creates a named tuple subclass stored in class variable `_assoc_factory`
            #
            # _assoc_factory is used in every instance to create tuple-like objects
            # that have fields accessible by attribute lookup as well as being indexable and iterable.
            # the tuple-like object can be accessed from self._body instance variable
            #
            # Each time we call change_heading() with different fields we reconstruct the _assoc_factory
            # and we can create different named tuples
            Association.change_heading(*list(kw_args.keys()))
        try:
            # A handle to access the named tuple
            self._body = self._assoc_factory(**kw_args)
            # Create @property names for each field of the association
            self._update_properties()
        except TypeError:
            raise AssociationError('Failed to construct Association because of the existing heading constraints')

    def _get_value(self, prop_name):
        """
        :param prop_name: property name
        :return: the value of property for the specific node
        """
        if prop_name in self.heading_fields:
            return getattr(self._body, prop_name)
        else:
            raise AssociationError(f'Failed to get value for property {prop_name}')

    def _update_properties(self):
        """
            Create public properties to access the keyword arguments of the Association
            `setattr` here will enable public access of these properties
        """
        # Update vertex properties
        [setattr(self, prop_name, self._get_value(prop_name)) for prop_name in self.heading_fields]

    @property
    def heading_fields(self):
        return Association._assoc_fields

    @property
    def body(self):
        return self._body

    def get(self):
        return self._body

    def __repr__(self):
        return self.body.__str__()
        # return f'A{self._hb}'

    def __str__(self):
        return self.body.__str__()
