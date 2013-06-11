import os, sys, inspect
import os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.sys.path.insert(0,parentdir)
from class_pedigree.scripts import progenitor_classes, ancestor_classes, child_classes

def test_progenitor_classes():
    import re
    class A(object): pass
    class B: pass
    class C(A): pass
    class D(C): pass
    class E(B,C): pass
    
    a_pro = progenitor_classes(A)
    d_pro = progenitor_classes(D)
    e_pro = set(progenitor_classes(E))
    if a_pro != [A]:
        print 'progenitor_classes failed self progenitor test'
        print "\t" + str(a_pro) + ' should be ' + str([A])
    if d_pro != [A]:
        print 'progenitor_classes failed grand-parent test:'
        print "\t" + str(d_pro) + ' should be ' + str([A])
    if e_pro != set([A,B]):
        print 'progenitor_classes failed dual inheritance test:'
        print "\t" + str(e_pro) + ' should be ' + str(set([A,B]))
    
def test_ancestor_classes():
    import re
    class A: pass
    class B: pass
    class C(A): pass
    class D(C): pass
    class E(B,A): pass
    
    a_anc = ancestor_classes(A)
    d_anc = set(ancestor_classes(D))
    e_anc = set(ancestor_classes(E))
    if a_anc != []:
        print 'ancestor_classes failed empty ancestors test.'
        print "\t" + str(a_anc) + ' should be empty'
    if d_anc != set([A,C]):
        print 'ancestor_classes failed grand-parent test.'
        print "\t" + str(d_anc) + ' should be ' + str([A,C])
    if e_anc != set([A,B]):
        print 'ancestor_classes failed dual inheritance test.'
        print "\t" + str(e_anc) + ' should be ' + str([A,B])
    
def test_child_classes():
    import re
    class A(object): pass
    class B(A): pass
    class C: pass
    class D(B,C): pass
    class E(C): pass
    
    clses = [A,B,C,D,E]
    a_chd = set(child_classes(A,clses))
    c_chd = set(child_classes(C,clses))
    e_chd = set(child_classes(E,clses))
    if e_chd != set([]):
        print 'child_classes failed empty child test.'
        print "\t" + str(e_chd) + ' should be empty'
    if a_chd != set([B,D]):
        print 'child_classes failed grand-parent test.'
        print "\t" + str(a_chd) + ' should be ' + str([B,D])
    if c_chd != set([D,E]):
        print 'child_classes failed sibling test.'
        print "\t" + str(c_chd) + ' should be ' + str([D,E])


test_progenitor_classes()
test_ancestor_classes()
test_child_classes()
