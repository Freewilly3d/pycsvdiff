######################################################################
#
# pycsvdiff.py - a differ for csv (comma separated) files
#
#
# The MIT License
#
# Copyright (c) 2008 Richard C. Harris (rconradharris<AT>gmail.com)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
######################################################################
import os
import sys
import pprint
import csv
import optparse
import StringIO
import unittest

################################################################
#
#                    Generic Table Diffing Code
#
################################################################
class Table(object):
    def __init__(self, rows, fields=None, label_first_row=False):
        """
        rows (iterator): something to yield rows like csv.reader
        fields (tuple): an optional set of field names. if ommitted,
            the fields will be auto-named with their field number.
        label_first_row (bool): the first row represents field labels
        """
        self.label_first_row = label_first_row
        self.rows = rows
        if fields:
            self.fields = fields
        else:
            peek = iter(rows).next()
            if label_first_row:
                self.fields = tuple(peek)
            else:
                fieldnames = map(lambda i: "%i" % i, range(len(peek)))
                self.fields = tuple(fieldnames)
                # Put the peeked value back into the stream
                def _iter():
                    yield peek
                    for row in rows:
                        yield row
                self.rows = _iter()


class TableDiffer(object):
    """
    Compare two table objects and return the difference

    The first step is to compare the schemas for the two tables, 
    call them a and b respectively.

    The schemas can differ in the following ways:

        field added:   j indexes b, j > len(a)
        field deleted: i indexes a, i > len(b)		
        field changed: a[i] != b[i] 

    We only compare rows between the tables if the schemas are exactly
    the same. Rows can differ in the following way:

        added   v indexes table_b's rows, v > len(table_a's rows) 
        deleted u indexes table_a's rows, u > len(table_b's rows)
        changed a[u] != b[u] which means that:
            there exists i such that a[u][i] != b[u][i] 
    """
    def __init__(self, table_a=None, table_b=None, 
                 skipped_fields=None,
                 verbosity=2,
                 ignore_case=False,
                 only_fields=None,
                 ignore_order=False):
        """
        table_a (Table): first table
        table_b (Table): second table
        skipped_fields (set): field numbers to ignore when diffing, these
            are relative to table_a
        verbosity (int): used to selectively switch on pretty-printers
        ignore_case (bool): make comparisons case insensitve
        only_fields (set): only use these fields when comparing
        ignore_order (bool): if tables are labeled then a between fields can
            be established so order can be ignored
        """
        self.table_a = table_a
        self.table_b = table_b
        self.difference_detected = False
        self.verbosity = verbosity
        self.ignore_case = ignore_case
        self.ignore_order = ignore_order
        if ignore_order:
            assert table_a.label_first_row == table_b.label_first_row == True
            
        all_fields = set(range(max(len(table_a.fields), len(table_b.fields))))
        assert not (skipped_fields and only_fields)
        if only_fields:
            skipped_fields = all_fields - set(only_fields)
        elif skipped_fields:
            skipped_fields = set(skipped_fields)
        else:
            skipped_fields = set()
            
        self.skipped_fields = skipped_fields
        # If we are using labels for fields, then create a mapping between
        # table_a's fields and table_b's in case we do use --ignore-order
        assert table_a.label_first_row == table_b.label_first_row
        self.mapping = set()
        if table_a.label_first_row:
            for i, field_a in enumerate(table_a.fields):
                for j, field_b in enumerate(table_b.fields):
                    if field_a == field_b:
                        self.mapping.add((i, j))
                        break
                    
            
    def _diff_row(self, row_a, row_b):
        """ 
        row_a (iterable): first row to diff
        row_b (iterable): second row to diff
        Returns: tuple representing differences
        """
        def comparator(p, q):
            if self.ignore_case:
                return str(p).lower() == str(q).lower()
            else:
                return p == q
        
        mapping = self.mapping if self.ignore_order else None
        diffs = diff_seq_by_index(row_a, row_b, 
                                  cmp=comparator,
                                  skip=lambda i: i in self.skipped_fields,
                                  mapping=mapping)
        
        # TODO: In order to allow different skip fields per table, must
        # rewrite sequence differ. It will then also support out of order
        # diffing which means I can add the --ignore-order option
        added = [(i, y) for i, x, y in diffs if x is nil]
        deleted = [(i, x) for i, x, y in diffs if y is nil]
        changed = [(i, x, y) for i, x, y in diffs 
                   if x is not nil and y is not nil]
        if any([added, deleted, changed]):
            self.difference_detected = True
        return added, deleted, changed

    def diff_fields(self):
        return self._diff_row(self.table_a.fields, self.table_b.fields)

    def diff_rows(self):
        """
        Iterates over rows in both tables in order to diff them
        Returns: Yields tuple representing row differences
        """
        a = iter(self.table_a.rows)
        b = iter(self.table_b.rows)
        i = 0
        added = []
        deleted = []
        changed = []
        while True:
            try:
                x = a.next()
            except StopIteration:
                x = nil
            try:
                y = b.next()
            except StopIteration:
                y = nil
            i += 1
            if x is nil and y is nil:
                break
            elif y is nil:
                yield "deleted", i-1, x
            elif x is nil:
                yield "added", i-1, y
            else:
                assert len(x) == len(y)
                _, _, values_changed = self._diff_row(x, y)
                if values_changed:
                    yield "changed", i-1, x, y, values_changed

    def output(self, data, out=sys.stdout, indent=0, verbosity=2):
        if self.verbosity >= verbosity:
            print >>out, " " * indent + data
        
    def pprint_value_changed(self, value_changed, out=sys.stdout,
                             indent=0):
        def output(data, out=out, indent=indent, verbosity=2):
            return self.output(data, out=out, indent=indent, 
                               verbosity=verbosity)
        i, x, y = value_changed
        field = self.table_a.fields[i]
        output("Value in field '%s' changed: '%s' -> '%s'" % (field, x, y))

    def pprint_values_changed(self, values_changed, out=sys.stdout, 
                              indent=0):
        for value_changed in values_changed:
            self.pprint_value_changed(value_changed, out=out, indent=indent)

    def pprint_row_diff(self, row_diff, out=sys.stdout, indent=0):
        def output(data, out=out, indent=indent, verbosity=1):
            return self.output(data, out=out, indent=indent, 
                               verbosity=verbosity)
           
        tag = row_diff[0]
        if tag == "added":
            _, i, row = row_diff
            output("Row %i added" %i)
            output(pprint.pformat(row))
        elif tag == "deleted":
            _, i, row = row_diff
            output("Row %i deleted" %i)
            output(pprint.pformat(row))
        elif tag == "changed":
            _, i, row_a, row_b, values_changed = row_diff
            output("Row %i changed" %i, out=out, indent=indent)
            self.pprint_values_changed(values_changed, out=out, 
                                       indent=indent+4)
        else:
            raise Exception("Unknown diff tag \"%s\"" % tag)
        output("-" * 50, verbosity=2)

    def pprint_row_diffs(self, out=sys.stdout, indent=0):
        for row_diff in self.diff_rows():
            self.pprint_row_diff(row_diff, out=out, indent=indent)

    def pprint_field_diffs(self, out=sys.stdout, indent=0):
        def output(data, out=out, indent=indent, verbosity=1):
            return self.output(data, out=out, indent=indent, 
                               verbosity=verbosity)
        
        added, deleted, changed = self.diff_fields()
        for x in added:
            output("Field '%s' added" % x)
        for x in deleted:
            output("Field '%s' deleted" % x)
        for x in changed:
            _, old, new = x
            output("Field changed: '%s' -> '%s'" % (old, new))

        return added, deleted, changed

    def pprint_diff(self, out=sys.stdout, indent=0):
        field_diffs = self.pprint_field_diffs(out=out, indent=indent)
        if any(field_diffs):
            self.output("Fields changed, skipping row diff!")
        else:
            self.pprint_row_diffs(out=out, indent=indent)


class NilType(object):
    """
    Nil is a singleton used to distinguish between a value being equal to
    None and value not being present.
    """
    def __repr__(self):
        return "nil"
    __str__ = __repr__


nil = NilType()

def diff_seq_by_index(a, b, 
                      cmp=lambda p, q: p == q, 
                      mapping=None,
                      skip=lambda i: False):
    """
    a and b (indexable, finite, iterables)
    cmp(func): a user-defined comparator function
    mapping (set of tuples): map a indices on to b
    skip (func): allows user to skip elements
    
    Returns: list of diff codes
    
    diff codes have the form: (idx, a_value, b_value)
    To differentiate between a_value or b_value being None and not
    existing within a sequence, a new object called nil is used.

    When diffing two sequences by index the following can occur:
    
            added:   i exists in b but not a              -> (i, nil, y)
            deleted: i exists in a but not b              -> (i, x, nil)
            changed: i exists in a and b but a[i] != b[i] -> (i, x, y)
    """
    mapping = set(mapping or [])
    # Convert tuple to bi-directional mapping in dict form
    mapping = dict(mapping | set(map(lambda x: tuple(reversed(x)), mapping))) 
    diffs = []
    for i in xrange(max(len(a), len(b))):
        if skip(i):
            continue
        try:
            x = a[i]
        except IndexError:
            x = nil
        try:
            j = mapping[i]
        except KeyError:
            j = i
        try:    
            y = b[j]
        except IndexError:
            y = nil
        if not cmp(x, y):			
            diffs.append((i, x, y))
    return diffs


################################################################
#
#                       Application Code
#
################################################################
def setup_options():
    USAGE = "%prog [options] a.csv b.csv"
    parser = optparse.OptionParser(USAGE)
    parser.add_option('-l', '--label',
                      action="store_true",  
                      dest="label_first_row",
                      default=False,
                      help="first row of data represents field labels")
    parser.add_option('-s', '--skip-fields',
                      action="store",  
                      dest="skipped_fields",
                      help="fields to skip (comma separated, use"
                           " @field_number or field_label if using -l)")
    parser.add_option('-i', '--ignore-case',
                      action="store_true",  
                      dest="ignore_case",
                      default=False,
                      help="ignore case when comparing field names and data")
    parser.add_option('-g', '--ignore-order',
                      action="store_true",  
                      dest="ignore_order",
                      default=False,
                      help="ignore the order of the columns if using labels")
    parser.add_option('-o', '--only-fields',
                      action="store",  
                      dest="only_fields",
                      help="use only these fields (same syntax as skip)")
    parser.add_option('-v', '--verbosity',
                      action="store",
                      type="int", 
                      dest="verbosity",
                      default=2,
                      help="level of verbosity to use (default: 2)")
    parser.add_option('--run-tests',
                      action="store_true",  
                      dest="run_tests",
                      help="run the test suite")

    options, args = parser.parse_args()
    if len(args) < 2 and not options.run_tests:
        help(parser)

    if options.ignore_order and not options.label_first_row:
        help(parser, "--ignore-order only makes sense for labeled tables!")
    return options, args

def get_table_from_csv(data, fields=None, label_first_row=False):
    return Table(csv.reader(data), fields=fields, 
                 label_first_row=label_first_row)	

def help(parser, data=None, out=sys.stdout, errcode=1):
    parser.print_help()
    if data is not None:
        print >>out, '\n' + data
    sys.exit(errcode)
    
def fatal(data, errcode=1, out=sys.stderr):
    print >>out, data
    sys.exit(errcode)

def get_files(args):
    file_a, file_b = args
    for file in (file_a, file_b):
        if not os.path.exists(file):
            fatal("file '%s' not found" % file)
    return file_a, file_b

def parse_fieldlist(fieldlist, table_a, table_b):
    def get_field_num(fieldname, table, table_num):
        matches = [(i, f) for i, f in enumerate(table.fields)
                   if f == fieldname]
        num_matches = len(matches)
        if num_matches == 1:
            return matches[0][0]
        elif num_matches > 1:
            fatal("Multiple fields found in table %i named '%s'. Use"
                  "field numbers to disambiguate" % (table_num, fieldname))
        else:
            fatal("Field '%s' not found in table %i" % (fieldname, table_num))

    if fieldlist:
        fieldset = set()
        for field in [i.strip() for i in fieldlist.split(',')]:
            if field.startswith('@'):
                field = field.lstrip('@')
                try:
                    field = int(field)
                except ValueError:
                    fatal("Unable to parse field number '%s'" % field)
                fieldset.add(field)
            elif field:
                field_num_a = get_field_num(field, table_a, 1)
                field_num_b = get_field_num(field, table_b, 2)
                assert field_num_a == field_num_b
                fieldset.add(field_num_a)
        return fieldset

def run_tests():
    loader = unittest.defaultTestLoader
    test_suite = loader.loadTestsFromModule(__import__('__main__'))
    test_runner = unittest.TextTestRunner(verbosity=1)
    result = test_runner.run(test_suite)
    return not result.wasSuccessful()

def main():
    options, args = setup_options()
    if options.run_tests:
        return run_tests()
    
    file_a, file_b = get_files(args)

    table_a = get_table_from_csv(open(file_a, "rb"), 
                                 label_first_row=options.label_first_row)
    table_b = get_table_from_csv(open(file_b, "rb"), 
                                 label_first_row=options.label_first_row)

    skipped_fields = parse_fieldlist(options.skipped_fields, table_a, 
                                     table_b)
    only_fields = parse_fieldlist(options.only_fields, table_a, table_b)
    differ = TableDiffer(table_a, table_b, 
                         skipped_fields=skipped_fields,
                         verbosity=options.verbosity,
                         ignore_case=options.ignore_case,
                         only_fields=only_fields,
                         ignore_order=options.ignore_order)
    differ.pprint_diff()
    return int(differ.difference_detected)
    
################################################################
#
#                          Tests
#
################################################################
class TestTable(unittest.TestCase):
    def test_fieldnames(self):
        rows = [(1,2,3), (4,5,6)]
        t = Table(rows)
        self.assertEqual(t.fields, ("0", "1", "2"))
        myfields = ("id", "name", "occupation")
        t = Table(rows, fields=myfields)
        self.assertEqual(t.fields, myfields)

class TestDiffSeqByIndex(unittest.TestCase):
    def assertDiff(self, a, b, expected):
        self.assertEqual(list(diff_seq_by_index(a, b)), expected)
    def test_same(self):
        self.assertDiff([1, 2, 3], [1, 2, 3], [])
    def test_added(self):
        self.assertDiff([1, 2], [1, 2, 3], [(2, nil, 3)])
    def test_deleted(self):
        self.assertDiff([1, 2, 3], [1, 2], [(2, 3, nil)])
    def test_changed(self):
        self.assertDiff([1, 2, 3], [1, 2, 4], [(2, 3, 4)])
    def test_added_changed(self):
        self.assertDiff([1, 2, 3], [1, 2, 4, 5], 
                        [(2, 3, 4), (3, nil, 5)])

class TestDiffSeqByIndexMapping(unittest.TestCase):
    def assertDiff(self, a, b, expected, mapping=None):
        self.assertEqual(list(diff_seq_by_index(a, b, mapping=mapping)), 
                         expected)
    def test_same(self):
        self.assertDiff([1, 2, 3], [1, 3, 2], [], 
                        mapping=[(1,2)])
    def test_added(self):
        self.assertDiff([1, 2], [1, 2, 3], [(1, 2, 3), (2, nil, 2)],
                        mapping=[(1,2)])
    def test_deleted(self):
        self.assertDiff([1, 2, 3], [1, 2], 
                        [(0, 1, 2), (1, 2, 1), (2, 3, nil)],
                        mapping=[(0,1)])
    def test_changed(self):
        self.assertDiff([1, 2, 3], [1, 3, 2], [], mapping=[(1,2)])
    def test_added_changed(self):
        self.assertDiff([1, 2, 3], [1, 2, 4, 5], 
                        [(2, 3, 4), (3, nil, 5)],
                        mapping=[])
        
        

class TestTableDifferDiffFields(unittest.TestCase):
    def assertFieldDiffs(self, table_a, table_b, added, deleted, changed):
        self.assertEqual(TableDiffer(table_a, table_b).diff_fields(),
                         (added, deleted, changed))
    def test_same(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        self.assertFieldDiffs(table_a, table_b, [], [], [])
    def test_added(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c", "d")], fields=(1, 2, 3, 4))
        self.assertFieldDiffs(table_a, table_b, [(3, 4)], [], [])
    def test_deleted(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b")], fields=(1, 2))
        self.assertFieldDiffs(table_a, table_b, [], [(2, 3)], [])
    def test_changed(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c")], fields=(1, 2, 4))
        self.assertFieldDiffs(table_a, table_b, [], [], [(2, 3, 4)])
    def test_added_changed(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c", "d")], fields=(1, 2, 4, 5))
        self.assertFieldDiffs(table_a, table_b, [(3, 5)], [], [(2, 3, 4)])


class TestTableDifferDiffRows(unittest.TestCase):
    def assertRowDiffs(self, table_a, table_b, expected):
        results = list(TableDiffer(table_a, table_b).diff_rows())
        self.assertEqual(results, expected)	
    def test_same(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        self.assertRowDiffs(table_a, table_b, [])	
    def test_added(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c"), ("e", "f", "g")], 
                        fields=(1, 2, 3))
        self.assertRowDiffs(table_a, table_b, 
                            [("added", 1, ("e", "f", "g"))])
    def test_deleted(self):
        table_a = Table(rows=[("a", "b", "c"), ("e", "f", "g")], 
                        fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        self.assertRowDiffs(table_a, table_b, 
                            [("deleted", 1, ("e", "f", "g"))])
    def test_changed(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "d")], fields=(1, 2, 3))
        self.assertRowDiffs(table_a, table_b, 
                            [("changed", 0, 
                              ("a", "b", "c"), ("a", "b", "d"), 
                              [(2, "c", "d")])])
    def test_added_changed(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "d"), ("e", "f", "g")], 
                        fields=(1, 2, 3))
        self.assertRowDiffs(table_a, table_b, 
                            [("changed", 0, 
                              ("a", "b", "c"), ("a", "b", "d"), 
                              [(2, "c", "d")]),
                              ("added", 1, ("e", "f", "g"))])


class TestTableDifferPPrintDiff(unittest.TestCase):
    def assertPPrintDiff(self, table_a, table_b, expected):
        out = StringIO.StringIO()
        TableDiffer(table_a, table_b).pprint_diff(out=out)
        self.assertEqual(out.getvalue(), expected)
    def test_same(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        self.assertPPrintDiff(table_a, table_b, "")
    def test_added(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c"), ("e", "f", "g")], 
                        fields=(1, 2, 3))
        expected = "Row 1 added\n('e', 'f', 'g')\n--------------------------------------------------\n"
        self.assertPPrintDiff(table_a, table_b, expected) 
    def test_deleted(self):
        table_a = Table(rows=[("a", "b", "c"), ("e", "f", "g")], 
                        fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        expected = "Row 1 deleted\n('e', 'f', 'g')\n--------------------------------------------------\n"
        self.assertPPrintDiff(table_a, table_b, expected)
    def test_changed(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "d")], fields=(1, 2, 3))
        expected = "Row 0 changed\n    Value in field '3' changed: 'c' -> 'd'\n--------------------------------------------------\n"
        self.assertPPrintDiff(table_a, table_b, expected)
    def test_added_changed(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "d"), ("e", "f", "g")], 
                        fields=(1, 2, 3))
        expected = "Row 0 changed\n    Value in field '3' changed: 'c' -> 'd'\n--------------------------------------------------\nRow 1 added\n('e', 'f', 'g')\n--------------------------------------------------\n"
        self.assertPPrintDiff(table_a, table_b, expected)


class TestTableDifferSkippedFields(unittest.TestCase):
    def assertFieldDiffs(self, table_a, table_b, added, deleted, changed,
                         skipped_fields=None):
        differ = TableDiffer(table_a, table_b, skipped_fields=skipped_fields)
        self.assertEqual(differ.diff_fields(), (added, deleted, changed))
    def test_same(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        self.assertFieldDiffs(table_a, table_b, [], [], [])
    def test_added(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c", "d")], fields=(1, 2, 3, 4))
        self.assertFieldDiffs(table_a, table_b, [], [], [],
                              skipped_fields=[3])
    def test_deleted(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b")], fields=(1, 2))
        self.assertFieldDiffs(table_a, table_b, [], [], [],
                              skipped_fields=[2])
    def test_changed(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c")], fields=(1, 2, 4))
        self.assertFieldDiffs(table_a, table_b, [], [], [],
                              skipped_fields=[2])
    def test_added_changed(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c", "d")], fields=(1, 2, 4, 5))
        self.assertFieldDiffs(table_a, table_b, [], [], [(2, 3, 4)],
                              skipped_fields=[3]) 
        self.assertFieldDiffs(table_a, table_b, [], [], [],
                              skipped_fields=[2, 3]) 

                              
class TestTableDifferIgnoreCase(unittest.TestCase):
    def assertRowDiffs(self, table_a, table_b, expected):
        differ = TableDiffer(table_a, table_b, ignore_case=True)
        results = list(differ.diff_rows())
        self.assertEqual(results, expected)	
    def test_same(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "C")], fields=(1, 2, 3))
        self.assertRowDiffs(table_a, table_b, [])

class TestTableDifferOnlyFields(unittest.TestCase):
    def assertFieldDiffs(self, table_a, table_b, added, deleted, changed,
                         only_fields=None):
        differ = TableDiffer(table_a, table_b, only_fields=only_fields)
        self.assertEqual(differ.diff_fields(), (added, deleted, changed))
    def test_same(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        self.assertFieldDiffs(table_a, table_b, [], [], [], 
                              only_fields=set([1]))
    def test_added(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c", "d")], fields=(1, 2, 3, 4))
        self.assertFieldDiffs(table_a, table_b, [], [], [],
                              only_fields=set([1]))
    def test_deleted(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b")], fields=(1, 2))
        self.assertFieldDiffs(table_a, table_b, [], [(2, 3)], [],
                              only_fields=set([2]))
    def test_changed(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c")], fields=(1, 2, 4))
        self.assertFieldDiffs(table_a, table_b, [], [], [(2, 3, 4)],
                              only_fields=set([1, 2]))
    def test_added_changed(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "b", "c", "d")], fields=(1, 2, 4, 5))
        self.assertFieldDiffs(table_a, table_b, [], [], [(2, 3, 4)],
                              only_fields=set([2]))
        
class TestTableDifferIgnoreOrder(unittest.TestCase):
    def assertRowDiffs(self, table_a, table_b, expected):
        table_a.label_first_row = table_b.label_first_row = True
        differ = TableDiffer(table_a, table_b, ignore_order=True)
        results = list(differ.diff_rows())
        self.assertEqual(results, expected)	
    def test_same(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "c", "b")], fields=(1, 3, 2))
        self.assertRowDiffs(table_a, table_b, [])	
    def test_added(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "c", "b"), ("e", "f", "g")], 
                        fields=(1, 3, 2))
        self.assertRowDiffs(table_a, table_b, 
                            [("added", 1, ("e", "f", "g"))])
    def test_deleted(self):
        table_a = Table(rows=[("a", "b", "c"), ("e", "f", "g")], 
                        fields=(1, 2, 3))
        table_b = Table(rows=[("a", "c", "b")], fields=(1, 3, 2))
        self.assertRowDiffs(table_a, table_b, 
                            [("deleted", 1, ("e", "f", "g"))])
    def test_changed(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "d", "b")], fields=(1, 3, 2))
        self.assertRowDiffs(table_a, table_b, 
                            [("changed", 0, 
                              ("a", "b", "c"), ("a", "d", "b"), 
                              [(2, "c", "d")])])
    def test_added_changed(self):
        table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
        table_b = Table(rows=[("a", "d", "b"), ("e", "f", "g")], 
                        fields=(1, 3, 2))
        self.assertRowDiffs(table_a, table_b, 
                            [("changed", 0, 
                              ("a", "b", "c"), ("a", "d", "b"), 
                              [(2, "c", "d")]),
                              ("added", 1, ("e", "f", "g"))])
        
################################################################
#
#                          Entry Point
#
################################################################
if __name__ == "__main__":
    sys.exit(main())
    
