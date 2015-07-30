## Description ##

**pycsvdiff** is a tool for diffing comma separated value (CSV) files. DBA's may find it useful to debug data-dumps, while programmers might integrate it with an automated testing solution to verify that data exports don't regress.

## Features ##

  * Will use field labels in diffs if specified
  * Ability to skip fields (e.g. timestamps) using field numbers or field labels as specifiers
  * Optional case insensitive diff
  * Can use labels to map out-of-order columns between CSV files
  * Variable verbosity
  * Built-in test suite runnable with --run-tests option from command line