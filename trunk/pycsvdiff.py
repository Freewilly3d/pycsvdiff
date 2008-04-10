import os
import sys
import pprint
import csv
import optparse
import StringIO

class Table(object):
	def __init__(self, rows, fields=None, label_first_row=False):
		"""
		rows (iterator): something to yield rows like csv.reader
		fields (tuple): an optional set of field names. if ommitted,
			the fields will be autonamed "field i" where i in 0 .. n
		label_first_row (bool): if the first row of data are actually
			labels store those as the field schema
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
	"""Compare two table objects and return the difference
	
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
	def __init__(self, table_a=None, table_b=None, ignored_fields=None):
		self.table_a = table_a
		self.table_b = table_b
		self.ignored_fields = ignored_fields or []
		self.difference_detected = False

	def diff(self):
		for field_diff in self.diff_fields():
			yield field_diff
		for row_diff in self.diff_rows():
			yield row_diff

	def _diff_row(self, row_a, row_b):
		diffs = diff_seq_by_index(row_a, row_b)
		ignored_fields = self.ignored_fields
		added = [(i, y) for i, x, y in diffs 
				 if x is nil and i not in ignored_fields]
		deleted = [(i, x) for i, x, y in diffs 
				   if y is nil and i not in ignored_fields]
		changed = [(i, x, y) for i, x, y in diffs 
				   if x is not nil and y is not nil 
					  and i not in ignored_fields]						
		if any([added, deleted, changed]):
			self.difference_detected = True
		return added, deleted, changed
	
	def diff_fields(self):
		return self._diff_row(self.table_a.fields, self.table_b.fields)
		
	def diff_rows(self):
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


	@staticmethod
	def output(data, out=sys.stdout, indent=0):
		print >>out, " " * indent + data
		
	def pprint_value_changed(self, value_changed, out=sys.stdout,
							 indent=0):
		i, x, y = value_changed
		field = self.table_a.fields[i]
		self.output("Value in field '%s' changed: '%s' -> '%s'" 
					% (field, x, y), out=out, indent=indent)

	def pprint_values_changed(self, values_changed, out=sys.stdout, 
							  indent=0):
		for value_changed in values_changed:
			self.pprint_value_changed(value_changed, out=out, indent=indent)
			
	def pprint_row_diff(self, row_diff, out=sys.stdout, indent=0):
		tag = row_diff[0]
		if tag == "added":
			_, i, row = row_diff
			self.output("Row %i added" %i, out=out, indent=indent)
			self.output(pprint.pformat(row), out=out, indent=indent)
		elif tag == "deleted":
			_, i, row = row_diff
			self.output("Row %i deleted" %i, out=out, indent=indent)
			self.output(pprint.pformat(row), out=out, indent=indent)
		elif tag == "changed":
			_, i, row_a, row_b, values_changed = row_diff
			self.output("Row %i changed" %i, out=out, indent=indent)
			self.pprint_values_changed(values_changed, out=out, 
									   indent=indent+4)
		else:
			raise Exception("Unknown diff tag \"%s\"" % tag)
		print >>out, "-" * 50

	def pprint_row_diffs(self, out=sys.stdout, indent=0):
		for row_diff in self.diff_rows():
			self.pprint_row_diff(row_diff, out=out, indent=indent)
	
	def pprint_field_diffs(self, out=sys.stdout, indent=0):
		added, deleted, changed = self.diff_fields()
		for x in added:
			self.output("Field '%s' added" % x, out=out, indent=indent)
		for x in deleted:
			self.output("Field '%s' deleted" % x, out=out, indent=indent)
		for x in changed:
			_, old, new = x
			self.output("Field changed: '%s' -> '%s'" % (old, new),
						out=out, indent=indent)
		return added, deleted, changed
		
	def pprint_diff(self, out=sys.stdout, indent=0):
		field_diffs = self.pprint_field_diffs(out=out, indent=indent)
		if any(field_diffs):
			self.output("Fields changed, skipping row diff!")
		else:
			self.pprint_row_diffs(out=out, indent=indent)
		
def diff_seq_by_value(a, b):
	"""a: iterable, b: iterable, returns: list of diff codes
	
	When diffing two sequences by value the following can occur:
	    i and j are indices into a and b respectively
	
		added: item in b and not in a -> (item, None, j)
		deleted: item in a and not in b -> (item, i, None)
		moved: item at i in a and j in b where a != j -> (item, i, j)
	"""
	added = [(x, None, j) for j, x in enumerate(b) if x not in a]
 	deleted = [(x, i, None) for i, x in enumerate(a) if x not in b]
	
	moved = []
	marked = set()
	for i, x in enumerate(a):
		for j, y in enumerate(b):
			if x == y and i != j and j not in marked:
				moved.append((x, i, j))
				marked.add(j)
				break
				
	return moved + deleted + added

class NilType(object):
	def __repr__(self):
		return "nil"
	__str__ = __repr__
nil = NilType()		
def diff_seq_by_index(a, b):
	"""a: iterable, b: iterable, returns: list of diff codes
	
	diff codes have the form: (idx, a_value, b_value)
	To differentiate between a_value or b_value being None and not
	existing within a sequence, a new object called nil is used.
	
	When diffing two sequences by index the following can occur:
		added:   i exists in b but not a              -> (i, nil, y)
		deleted: i exists in a but not b              -> (i, x, nil)
		changed: i exists in a and b but a[i] != b[i] -> (i, x, y)
	"""
	a_, b_ = iter(a), iter(b)
	i = 0
	diffs = []
	while True:
		try:
			x = a_.next()
		except StopIteration:
			x = nil
		try:
			y = b_.next()
		except StopIteration:
			y = nil
		i += 1
		if x is nil and y is nil:
			break
		elif x != y:			
			diffs.append((i-1, x, y))		
	return diffs
	
	
import unittest
class TestTable(unittest.TestCase):
	def test_fieldnames(self):
		rows = [(1,2,3), (4,5,6)]
		t = Table(rows)
		self.assertEqual(t.fields, ("0", "1", "2"))
		myfields = ("id", "name", "occupation")
		t = Table(rows, fields=myfields)
		self.assertEqual(t.fields, myfields)

class TestDiffSeqByValue(unittest.TestCase):
	def assertDiff(self, a, b, expected):
		self.assertEqual(diff_seq_by_value(a, b), expected)
	def test_same(self):
		self.assertDiff([1, 2, 3], [1, 2, 3], [])
	def test_added(self):
		self.assertDiff([1, 2], [1, 2, 3], [(3, None, 2)])
	def test_deleted(self):
		self.assertDiff([1, 2, 3], [1, 2], [(3, 2, None)])

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

def setup_options():
	USAGE = "%prog [options] a.csv b.csv"
	parser = optparse.OptionParser(USAGE)
	parser.add_option('-l', '--label-first-row',
					  action="store_true",  
					  dest="label_first_row",
				      default=False,
					  help="first row of data are field labels")
	parser.add_option('-i', '--ignored-fields',
					  action="store",  
					  dest="ignored_fields",
					  help="fields to ignore (comma separated, use"
					        " @field_number or field_label if using -l)")
	parser.add_option('--run-tests',
					  action="store_true",  
					  dest="run_tests",
					  help="run the test suite")

	options, args = parser.parse_args()
	if len(args) < 2 and not options.run_tests:
		parser.print_help()
		sys.exit(1)
		
	return options, args

def get_table_from_csv(data, fields=None, label_first_row=False):
	return Table(csv.reader(data), fields=fields, 
				 label_first_row=label_first_row)	

def fatal(data, errcode=1, out=sys.stderr):
	print >>out, data
	sys.exit(errcode)
	
def check_file(file):
	if not os.path.exists(file):
		fatal("file '%s' not found" % file)

def parse_ignored_fields(options, table_a, table_b):
	ignored_fields = options.ignored_fields
	
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
			
	if ignored_fields:
		to_ignore = []
		ignored_fields = ignored_fields.split(',')
		ignored_fields = [i.strip() for i in ignored_fields]
		for field in ignored_fields:
			if field.startswith('@'):
				field = field.lstrip('@')
				try:
					field = int(field)
				except ValueError:
					fatal("Unable to parse field number '%s'" % field)
				to_ignore.append(field)
			elif field:
				field_num_a = get_field_num(field, table_a, 1)
				field_num_b = get_field_num(field, table_b, 2)
				assert field_num_a == field_num_b
				to_ignore.append(field_num_a)
		return to_ignore
	return None

def run_tests():
	loader = unittest.defaultTestLoader
	test_suite = loader.loadTestsFromModule(__import__('__main__'))
	test_runner = unittest.TextTestRunner(verbosity=1)
	result = test_runner.run(test_suite)
	return not result.wasSuccessful()
	
	print test
def main():
	options, args = setup_options()
	if options.run_tests:
		return run_tests()
	file_a, file_b = args
	check_file(file_a)
	check_file(file_b)
	
	table_a = get_table_from_csv(open(file_a, "rb"), 
								label_first_row=options.label_first_row)
	table_b = get_table_from_csv(open(file_b, "rb"), 
								label_first_row=options.label_first_row)
	
	ignored_fields = parse_ignored_fields(options, table_a, table_b)
	differ = TableDiffer(table_a, table_b, ignored_fields=ignored_fields)
	differ.pprint_diff()
	return int(differ.difference_detected)
	
if __name__ == "__main__":
	sys.exit(main())
	
	#unittest.main()
	#table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
	#table_b = Table(rows=[("a", "b", "c"), ("e", "f", "g")], 
	#				fields=(1, 2, 3))
	#differ = TableDiffer(table_a, table_b)
	#differ.pprint_row_diffs()
	

	#table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
	#table_b = Table(rows=[("a", "b", "d")],	fields=(1, 2, 3))
	#differ = TableDiffer(table_a, table_b)
	#differ.pprint_diff()
	
	#table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
	#table_b = Table(rows=[("a", "b", "d", "f")], fields=(1, 2, 3, 4))
	#differ = TableDiffer(table_a, table_b)
	#differ.pprint_diff()

	#table_a = Table(rows=[("a", "b", "c")], fields=(1, 2, 3))
	#table_b = Table(rows=[("a", "b", "c")], fields=(1, 2, 4))
	#differ = TableDiffer(table_a, table_b)
	#differ.pprint_diff()
	
	#a = get_table_from_csv(open(sys.argv[1], "rb"), label_first_row=True)
	#b = get_table_from_csv(open(sys.argv[2], "rb"), label_first_row=True)
	#differ = TableDiffer(a, b)
	#differ.pprint_diff()
	#print a.fields, b.fields