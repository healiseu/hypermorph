"""
HyperMorph Modules Testing: Association construct
(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph.hassoc import Association

a_init = Association(hb=(), prtcol=None, prtwgt=None, prtID=None, prtnam=None, prtunt=None)
a1 = Association(hb=(7, 4), prtID=227, prtnam='car battery')
a2 = Association((7, 4), None, None, 227, 'car battery', None)

a2
a2.body

a1
a1.body
a1.heading_fields
a1.get()
print(a1)
(a1.prtnam, a1.prtcol, a1.prtID)

Association.change_heading('hb', 'supID', 'supName', 'supAddress')
# Make it to fail with an association that has inconsistent heading
Association(hb=(1, 1), prtID=227, prtnam='car battery')

Association(hb=(1, 2), supID=227, supName='Acme Widgets')


# For sorting associations see...
# https://alysivji.github.io/python-sorting-multiple-attributes.html
# https://docs.python.org/3/howto/sorting.html#sortinghowto