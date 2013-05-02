=====================================================
Fast OpenERP migration tool using CSV export / import
=====================================================

.. warning:: Reminder
    This tool is highly experimental and is distributed in the hope that it
    will be useful, but WITHOUT ANY WARRANTY of FITNESS FOR A PARTICULAR
    PURPOSE. In particular, if you're using OpenERP for your company, you
    should consider purchasing an OpenERP Enterprise contract that will
    provide you with the best and riskless migration path for your database
    and custom developments.

This tool has been developped with these initial goals in mind:

 - Migrating from OpenERP 6.1 to 7.0
 - Merging 2 different OpenERP databases (into a single multicompany db)
 - Migrating data from a legacy business application (Access, Delphi, etc.)
 - Migrating from Dolibarr to OpenERP

The principle of this tool is to export CSV data from another application, then
to process CSV files in order to import it into a freshly installed OpenERP
database. This is a completely different strategy than the in-place migration
of OpenERP or OpenUpgrade. This tool is focusing only on important data, and
assume the custom code has already been migrated.

Installation
============

You should install this tool in a virtualenv or buildout::

    $ virtualenv sandbox
    $ sandbox/bin/pip install anybox.migration.openerp


Usage
=====

This tool offer a single "migrate" script::

    $ sandbox/bin/migrate -s source_dbname -t target_dbname

If you want to inpect the temporary CSV files created, use the --keepcsv
option. They will be stored in a temporary directory under the current
directory.


Internals
=========

This tool was inspired from:

 - external_referential
 - OpenUpgrade
 - Talend Open Studio

The different internal steps are:

 - exporting CSV from the old database
 - Transforming CSV to match the target database
 - reinjecting into OpenERP in the right order (possibly in 2-pass)

The transformation of CSV files is done using a simple mapping file written in Yaml:

TODO: mapping example with different scenario (splitting lines, generating new
ids, modifying output, etc.)

Contribute
==========

Authors and contributors:

 - Christophe Combelles
 - Florent Jouatte
 - Guy-Clovis Nzouendjou

Code

 - Code repository and bug tracker: https://bitbucket.org/anybox/anybox.migration.openerp
 - PyPI page: http://pypi.python.org/pypi/anybox.migration.openerp

Please don't hesitate to give us feedback, report bugs or contribute the mapping files
on Bitbucket.

