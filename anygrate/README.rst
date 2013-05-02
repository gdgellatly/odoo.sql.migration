=======================================
anybox.migration.openerp design doctest
=======================================

To run the tests:

    $ virtualenv sandbox
    $ ./sandbox/bin/python setup.py develop
    $ ./sandbox/bin/python setup.py test


Installing required modules on the target database
==================================================

The target database should be as close as possible as the source database.
The mapping depend on the installed modules

Selecting a target table
========================

The first step consists in selecting a target OpenERP model we want to import.
We'll take two examples for this doctest: 'res.partner' and 'res.users'.

Determining the dependent tables
================================

A target table such as 'res_users' depends on several ones through foreign
keys: 'res_partner', 'mail_alias', etc.  When importing 'res_users', we must
also import the dependent child tables.  So we must get a list of tables to
import, and this list should be ordered so we get the child tables first.

    >>>


Building a mapping
==================

a source table can correspond to several ones on the target database,
or several source tables can correspond to a single one on the target.
Same applies for the fields.
So we can read a general mapping stored in a file.
Given a source table or csv file, this mapping should be able to return the target tables.

We read the mapping:

    >>> from anygrate.mapping import Mapping
    >>> import os, anygrate
    >>> from os.path import join, dirname
    >>> testdir =  join(dirname(anygrate.mapping.__file__), 'test')
    >>> test_file = join(testdir, 'test_mapping.yml')
    >>> mapping = Mapping(['base'], test_file)

After selecting a target table, a mapping is constructed from the different
mappings contained in the different modules, and gives the target tables and
fields, given a source table.

Now we can get the target tables from a source table, given a module:

    >>> mapping.get_targets('res_users')
    ['res_users', 'res_partner']

Or the source tables from a target table:

    >>> mapping.get_sources('res_partner')
    ['res_partner', 'res_partner_address', 'res_users']

And the target tables and fields from a source field:

    >>> from pprint import pprint
    >>> pprint(mapping.get_targets('res_users.login'), width=1)
    {'res_partner.name': <function mapping_function at ...,
     'res_users.name': <function mapping_function at ...}


    >>> pprint(mapping.get_targets('res_users.address_id'), width=1)
    {'res_partner.id': <function mapping_function at ...}

    >>> mapping = Mapping(['base', 'mail'], test_file)
    >>> pprint(mapping.get_targets('res_users.login'), width=1)
    {'mail_alias.alias': <function mapping_function at ...,
     'res_partner.name': <function mapping_function at ...,
     'res_users.name': <function mapping_function at ...}

    >>> mapping = Mapping(['base'], test_file)

This means the 'name' column is unchanged:

    >>> mapping.get_targets('res_partner.name')
    {'res_partner.name': None}

this means: the 'date' column is just renamed to login_date:

    >>> mapping.get_targets('res_partner.date')
    {'res_partner.login_date': None}

We can use wildcards in the mappings to avoid filling every column:

    >>> wildcard = Mapping(['base'], join(testdir, 'wildcard.yml'))
    >>> partial_wildcard = Mapping(['base'], join(testdir, 'partial_wildcard.yml'))
    >>> wildcard.get_targets('foo.bar')
    {'foo.bar': None}
    >>> partial_wildcard.get_targets('res_users.password')
    {'res_users.password': <function mapping_function at ...>}
    >>> partial_wildcard.get_targets('res_users.plop')
    {'res_users.plop': None}
    >>> partial_wildcard.get_targets('res_partner_address.plop')
    {'res_partner.plop': None}
    >>> partial_wildcard.get_targets('res_partner_address.name')
    {'res_partner.name': <function mapping_function at ...>}



Exporting CSV data
==================

We must be able to export the source tables :

    >>> source_tables = ['res_users', 'res_partner']
    >>> from anygrate import exporting
    >>> from tempfile import mkdtemp
    >>> directory = mkdtemp()
    >>> import psycopg2
    >>> connection = psycopg2.connect("dbname=test")
    >>> exporting.export_tables(source_tables, directory, connection)
    ['/tmp/.../res_users.csv', '/tmp/.../res_partner.csv']
    >>> sorted(os.listdir(directory))
    ['res_partner.csv', 'res_users.csv']

Processing csv files
====================

The exported csv files should now be processed with the mapping, so that new
csv files be generated

    >>> from anygrate.processing import CSVProcessor
    >>> processor = CSVProcessor(mapping)
    >>> filepaths = [join(directory, 'res_users.csv')]
    >>> pprint(processor.get_target_columns(filepaths), width=1)
    {'res_partner': set(['id',
                         'name']),
     'res_users': set(['name',
                       'partner_id'])}
    >>> processor.process(directory, ['res_users.csv'], directory)
    >>> sorted(os.listdir(directory))
    ['res_partner.csv', 'res_partner.out.csv', 'res_users.csv', 'res_users.out.csv']
    >>> import csv
    >>> sorted(csv.DictReader(open(join(directory, 'res_users.out.csv'))).next().keys())
    ['name', 'partner_id']

We can try more complex scenarios, such as:

- res_users split into res_partner + res_users
- res_partner merge from res_partner + res_partner_address

    >>> directory2 = mkdtemp()
    >>> processor.process(testdir, ['res_users.csv', 'res_partner.csv', 'res_partner_address.csv'], directory2)
    >>> sorted(os.listdir(directory2))
    ['res_partner.out.csv', 'res_users.out.csv']


Importing the CSV files
=======================

Before importing, existing init data should be matched to csv data if possible.
or before importing, foreign keys should be applied an offset?

Now we can import a csv file using the mapping:

    >>> from anygrate import importing
    >>> importing.import_csv(join(directory, 'res_users.csv'), connection)
    Traceback (most recent call last):
    ...
    IntegrityError: ...
    >>> import shutil
    >>> shutil.rmtree(directory)
    >>> shutil.rmtree(directory2)


