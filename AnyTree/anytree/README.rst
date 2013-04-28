=============================================
OpenERP 7 migration using CSV import / export
=============================================

The goal of this tool is to export CSV data from another application (OpenERP
6.1, OpenERP 7, Dolibarr), then to process CSV files in order to import it into
a freshly installed OpenERP database.  This is a different strategy than the
in-place migration from OpenERP or OpenUpgrade.

The migration process involves different steps and tools introduced below.

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

    >>> from anytree.mapping import Mapping
    >>> import os, anytree
    >>> from os.path import join, dirname
    >>> test_file = join(dirname(anytree.mapping.__file__), 'test', 'test_mapping.yml')
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
    {'res_partner.name': "return lines['res_users']['login']",
     'res_users.name': "return lines['res_users']['login'].lower()"}


    >>> pprint(mapping.get_targets('res_users.address_id'), width=1)
    {'res_partner.id': "return lines['res_users']['address_id']"}

    >>> mapping = Mapping(['base', 'mail'], test_file)
    >>> pprint(mapping.get_targets('res_users.login'), width=1)
    {'mail_alias.alias': "return lines['res_users']['login']",
     'res_partner.name': "return lines['res_users']['login']",
     'res_users.name': "return lines['res_users']['login'].lower()"}


The mapping is explicit: all the target columns is present.
It also allows to compute the output columns:

    >>> pprint(mapping.out_columns, width=1)
    {'mail_alias': set(['alias']),
     'res_partner': set(['id',
                         'login_date',
                         'name',
                         'parent_id']),
     'res_users': set(['name'])}

    >>> mapping = Mapping(['base'], test_file)

This means the 'name' column is unchanged:

    >>> mapping.get_targets('res_partner.name')
    {'res_partner.name': None}

this means: the 'date' column is renamed to login_date:

    >>> mapping.get_targets('res_partner.date')
    {'res_partner.login_date': None}


Exporting CSV data
==================

We must be able to export the source tables :

    >>> source_tables = ['res_users', 'res_partner']
    >>> from anytree import exporting
    >>> from tempfile import mkdtemp
    >>> destination_dir = mkdtemp()
    >>> exporting.export_tables(source_tables, dest_dir=destination_dir, db="test")
    >>> sorted(os.listdir(destination_dir))
    ['res_partner.csv', 'res_users.csv']

Processing csv files
====================

The exported csv files should now be processed with the mapping, so that new
csv files be generated

    >>> from anytree.processing import CSVProcessor
    >>> processor = CSVProcessor(mapping)
    >>> processor.process(join(destination_dir, 'res_users.csv'))
    >>> sorted(os.listdir(destination_dir))
    ['res_partner.csv', 'res_users.csv', 'res_users.out.csv']


Importing the CSV files
=======================

Before importing, existing init data should be matched to csv data if possible.
or before importing, foreign keys should be applied an offset?

Now we can import a csv file using the mapping:

    >>> from anytree import importing
    >>> importing.import_csv(join(destination_dir, 'res_users.csv'))
    Traceback (most recent call last):
    ...
    IntegrityError: ...
    >>> import shutil; shutil.rmtree(destination_dir)


