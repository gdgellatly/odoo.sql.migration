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

    >>> from anytree import mapping
    >>> from os.path import join, dirname
    >>> test_file = join(dirname(mapping.__file__), 'test', 'test_mapping.yml')
    >>> mapping = mapping.Mapping(test_file)

After selecting a target table, a mapping is constructed from the different
mappings contained in the different modules, and gives the target tables and
fields, given a source table.

Now we can get the target tables from a source table, given a module:

    >>> mapping.get_targets(['base'], 'res_users')
    ['res_users', 'res_partner', 'mail_alias']

Or the source tables from a target table:

    >>> mapping.get_sources(['base'], 'res_partner')
    ['res_partner', 'res_partner_address']

And the target tables and fields from a source field:

    >>> from pprint import pprint
    >>> pprint(mapping.get_targets(['base'], 'res_users.login'), width=1)
    {'res_users': {
        'login': None
        'name' "return lines['res_users']['login'].lower()"
     'res_partner': {
        'name': "return lines['res_users']['login']"},
    }
    >>> pprint(mapping.get_targets(['base' 'mail'], 'res_users.login'), width=1)
    {'res_users': {
        'login': None
        'name' "return lines['res_users']['login'].lower()"
     'res_partner': {
        'name': "return lines['res_users']['login']"},
     'mail_alias': {
        'alias': "return lines['res_users']['login']"},
    }

    >>> pprint(mapping.get_targets(['base'], 'res_users', 'address_id'), width=1)
    {'res_partner': 'id'}

The mapping shows only unexpected mappings: if all the fields and columns are the same, the mapping is empy.

    >>> mapping.get_targets(['base'], 'res_partner.name')
    >>> mapping.get_targets(['base'], 'res_partner.date')
    {'res_partner': 'login_date'}



Exporting CSV data
==================

We must be able to export the source tables :

    >>> source_tables = ['res_users', 'res_partner', 'mail_alias']
    >>> from anytree import exporting
    >>> from tempfile import mkdtemp
    >>> destination_dir = mkdtemp()
    >>> exporting.export_tables(source_tables, dest=destination_dir)
    >>> ordered(os.listdir(destination_dir))
    ['mail_alias.csv', 'res_partner.csv', 'res_users.csv']

Processing and importing the CSV files
======================================

Before importing, existing init data should be matched to csv data if possible.
or before importing, foreign keys should be applied an offset?

Now we can import a csv file using the mapping:

    >>> from anytree import importing
    >>> importing.import_csv(join(destination_dir, 'mail_alias.csv')


