================================
Fast OpenERP migration framework
================================

.. warning:: Reminder
    This tool is complex and highly experimental and is distributed in the hope
    that it **might** be useful, but WITHOUT ANY WARRANTY of FITNESS FOR A
    PARTICULAR PURPOSE. In particular, if you're using OpenERP for your company,
    you should consider purchasing an OpenERP Enterprise contract that will provide
    you with the best and riskless migration path for your database and custom
    developments.

This tool has been developped with these initial goals in mind:

 - **Migrating** from OpenERP 6.1 to 7.0
 - **Merging** 2 different OpenERP databases into a single multicompany db
 - Migrating data from a legacy business application (Access, Delphi, etc.)
 - Migrating from Dolibarr to OpenERP

The principle of this tool is to export CSV data from another application, then
to process CSV files in order to import them into a freshly installed OpenERP
database. This is a completely different strategy than the in-place migration
of OpenERP or OpenUpgrade.

Installation
============

You should install this tool in a virtualenv or buildout::

    $ hg clone https://bitbucket.org/anybox/anybox.migration.openerp
    $ cd anybox.migration.openerp
    $ virtualenv sandbox
    $ sandbox/bin/python setup.py install


Usage
=====

This tool offer a single "migrate" script::

    $ sandbox/bin/migrate -h


You should select the source and target DBs, and a selection of wanted tables to migrate.
The script then takes care of selecting the dependant tables::

    $ sandbox/bin/migrate -s source_dbname -t target_dbname -m res_partner account_move

If you want to inspect the temporary CSV files created, use the --keepcsv
option. They will be stored in a temporary directory under the current
directory.

This script won't actually do anything unless you specify the ``-w`` option to
commit the transaction at the end.

The most important part of the migration is the YML mapping file, which
describes how to handle data table by table and column by column. A default
mapping file is provided and is being used as a real mapping for a migration
consisting in migrating two 6.1 databases into a single 7.0 multicompany
database.  Future versions will allow to select a different mapping file, or to
use several of them.


Internals
=========

This tool was very loosely inspired from:

 - the external_referential OpenERP module
 - the OpenUpgrade project
 - Talend Open Studio

The different internal steps are:

 - Exporting CSV from the old database
 - Transforming CSV to match the target database
 - Detect data existing in the target DB with discriminators
 - Postprocessing CSV files to fix foreign keys
 - Reinjecting into OpenERP
 - Updating possible pre-existing data with incoming data

The transformation of CSV files is done using a mapping file written in Yaml.

Mapping file
============

You should always have in mind that the migration is only handling low-level
database tables and columns, the OpenERP fields are unknown to this tool. Each
table, each line, each cell of the source database is handled independently and
the mapping file tells what to do with the current cell.

Copying data
------------

The most simple and basic YML directive for a column is the following::

    module:
        table1.column1:
            table2.column2: __copy__

It defines that if the OpenERP ``module`` is installed in the **target** database, the
``column1`` of the ``table1`` from the source DB should be copied
onto the ``column2`` of the ``table2`` of the target DB.

the ``__copy__`` instruction can be omitted and the previous directive is
equivalent to this one::

    module:
        table1.column1:
            table2.column2:

Internally, this directive is actually converted to a Python dict::

    {'module':
        {'table1.column1':
            {'table2.column2': '__copy__'}}

Copying all columns of a table
------------------------------
If your target table has the same structure as the source table, you can avoid specifying one directive for each column and use a wildcard::

    module:
        table1.*:

It means: copy all the columns from table1 of the source db to table1 in the target db.

Copying all columns to a different table
----------------------------------------
If the source table has just been renamed, you can copy all the columns of the source table1 to the target table2::

    module:
        table1.*:
            table2.*:

Copying everything
------------------
If the source and target db have exactly the same structure and you just want to transfer data, you can use a global wildcard::

    module:
        .*:

It means: copy all tables to the target database without transformation


Transforming data with Python code
----------------------------------

Instead of a basic data copy with the ``__copy__`` directive, you can use any Python code

TODO internal variables:
source_row
target_rows
newid()

Splitting a source line to several target lines
-----------------------------------------------

TODO several targets

Not migrating a column
----------------------

If you want to get rid of a specific column in a table::

    module:
        table1.column1: __forget__

TODO

Feeding a new column
--------------------

TODO

Merging with existing data
--------------------------

TODO explain __discriminator__

Foreign keys without constraint
-------------------------------
(related with store)

TODO explain __fk__

Handle cyclic foreign keys
--------------------------
(This directive is not yet implemented)

TODO explain __defer__

Contribute
==========

Authors and contributors:

 - Christophe Combelles
 - Florent Jouatte
 - Guy-Clovis Nzouendjou

Code

 - Code repository and bug tracker: https://bitbucket.org/anybox/anybox.migration.openerp

Please don't hesitate to give us feedback, report bugs or contribute the mapping files
on Bitbucket.

