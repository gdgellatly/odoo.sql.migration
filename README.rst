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

This tool has been developped with these initial goals in mind, in this
priority order:

 - **Merging** 2 different OpenERP databases into a single multicompany db
 - **Migrating** from OpenERP 6.1 to 7.0
 - Migrating data from a legacy business application (Access, Delphi, etc.)
 - Migrating from Dolibarr to OpenERP

The principle of this tool is to export CSV data from an old application (only
OpenERP for now), then to process CSV files in order to import them into a
freshly installed OpenERP database. This is a completely different strategy
than the in-place migration of OpenERP or OpenUpgrade. It also requires some
treatment after the migration, such as recreating internal sequences.  Import
and export are done with the PostgreSQL-specific COPY command, and results in
extremely fast exports and imports. Combined with a pure in-memory Python csv
processing, this tool can often achieve overall migration rates over 1000
lines/sec.

Installation
============

This tool only works with **Python 2.7**!

You can install this tool in a virtualenv or buildout::

    $ virtualenv sandbox
    $ sandbox/bin/pip install anybox.migration.openerp


Usage
=====

This tool offers a single ``migrate`` script::

    $ sandbox/bin/migrate -h

You can list the available default mapping files::


    $ sandbox/bin/migrate -l
    openerp6.1-openerp7.0.yml

You should specify the source and target DBs, a selection of the source tables
to migrate, and the mapping files to use.  The tool then takes care of
selecting the dependant tables::

    $ sandbox/bin/migrate -s source_dbname -t target_dbname -r res_partner account_move -p openerp6.1-openerp7.0.yml custom.yml

If you want to inspect the temporary CSV files created, use the ``--keepcsv``
option. They will be stored in a temporary directory under the current
directory.

This script won't actually write anything in the target database unless you
specify the ``-w`` option to commit the transaction at the end.

The most important part of the migration process is the YML mapping file, which
describes how to handle data, table by table and column by column. A default
mapping file is provided and is being used as a real mapping for a migration
consisting in migrating two 6.1 databases into a single 7.0 multicompany
database.  You can mix the default 6.1 to 7.0 file provided, and augment it
with other custom yml files, they will be merged.


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

The processing of CSV files is done using a mapping file written in Yaml.

Mapping file
============

You should keep in mind that this migration tool is only dealing with database
tables and columns: the OpenERP fields are unknown to it. Each table,
each line, each cell of the source database is handled independently and the
mapping file tells what to do with the current cell. This leads to limitations
and this tool won't be able to handle extremely complex migration.  But it
is powerful enough to allow to simultaneously merge and migrate two 6.1
databases into a 7.0 multicompany database.

For a real-life example, you can have a look at the OpenERP 6.1 to 7.0 mapping
file provided in the ``mappings`` directory of this tool.

Copying data
------------

The most simple and basic YML statement for a column mapping is the following::

    module:
        table1.column1:
            table2.column2: __copy__

It tells that, if the OpenERP ``module`` is installed in the **target**
database, the ``column1`` of the ``table1`` from the source DB should be copied
to the ``column2`` of the ``table2`` in the target DB.

The ``__copy__`` instruction can even be omitted and the previous statement is
equivalent to this one::

    module:
        table1.column1:
            table2.column2:

Internally, this statement is actually converted to a Python dict::

    {'module':
        {'table1.column1':
            {'table2.column2': '__copy__'}}

And the whole yml file is converted to a large mapping dict whose leafs are
statements or functions which are able to process data.

Copying all columns of a table
------------------------------

If your target table has the same structure as the source table, you can avoid
specifying one mapping statement for each column and use a wildcard::

    module:
        table1.*:

It means: copy all the columns of table1 from the source db to table1 in the
target db.  This kind of mapping is often used as a starting point when source
and table structures are similar. You can then add mapping statements for
specific columns to override this wildcard.

Copying all columns to a different table
----------------------------------------

If the source table has only been renamed, you can copy all the columns of the
source table1 to the target table2::

    module:
        table1.*:
            table2.*:

Copying everything
------------------

If the source and target db have exactly the same structure and you just want
to transfer data, you may use a global wildcard (but we have not had the
opportunity to try this one for real yet)::

    module:
        .*:

It means: copy all tables to the target database without processing. It may
seem unuseful compared to a bare dump and restore, but remind that this way you
can append data to the target DB, not only replace it. In that case you should
take care of existing data, if the table has constraints (see discriminators
below)

Splitting one source line to several tables
-------------------------------------------

For a single source line coming from a source table, you can feed data in
several target tables. This can be done just by putting several target lines
like this::

    module:
        table1.column1:
            table2.column2:
            table3.column3:

It means: for each ``column1`` in the ``table1`` of the source DB, create two
target lines: one for ``table2`` and one for ``table3``.

During the processing of the current line, other mapping statements
can feed the same target lines. Take this example::

    module:
        table1.column1:
            table2.column2:
            table3.column3:
        table1.column2:
            table2.column2:
            table3.column4:

In this case, data in the ``table1`` will be directed to ``table2`` and
``table3``. You can then add more lines to handle all the columns of ``table1``

However in the example above, there is a conflict since two source cells are directed
to the same target cell (``table2.column2``). In this scenario, there is no way to
predict which one will be used (because the mapping is a Python *dict* and a dict is not
ordered). You should avoid this kind of conflicts.

In case of an OpenERP 6.1 to 7.0 migration, this kind of mapping is actually
used to migrate one source ``res_users`` line to three different lines: one in
``res_users`` + one in ``res_partner`` + one in ``mail_alias``. See the default
mapping for a real example.

Not migrating a column
----------------------

If you want to get rid of a specific column in a table, use the ``__forget__``
statement::

    module:
        table1.column1: __forget__

This statement is useful if you defined a wildcard, to prevent from migrating a
specific column.

Transforming data with Python code
----------------------------------

Instead of just copying data with the ``__copy__`` statement, you can use any
Python code. The Python code should be written in a literal Yaml block and is
executed as is, as a function body, so that you have to insert a ``return``
statement somewhere.

Example from the ``mail`` module::

    mail:
        mail_message.type:
            mail_message.type: return 'email'

It means the ``type`` column of the ``mail_message`` table will be filled with
``'email'`` strings, whatever data the source column had.
        
The eventual signature of the function constructed using the Python code block is ::

    def mapping_function(self, source_row, target_rows):

It means that in the function body you can access the full ``source_row``,
which is a dict containing all the keys (column names) and values of the
current line being processed. But keep in mind that at this time, you are
dealing with one specific cell of this line, and you should return the value
that will be inserted in the corresponding cell of the target table. This can
be used to aggregate data from two source cells into a target cell::

    base:
        table1.firstname: __forget__
        table1.name:
            table1.name: return source_row['firstname'] + ' ' + source_row['name']

You can also access the ``target_rows`` beeing filled during the processing of
the line, so that data coming from a source cell can influence several cells in
the target lines, or even different target tables. Here is an example::

    base:
        table1.id:
            table1.id:
            table2.id:
        table1.name:
            table1.name: |
                name = source_row['firstname'] + ' ' + source_row['name']
                target_rows['table1']['display_name'] = name
                target_rows['table2']['display_name'] = name
                return name
            table2.name

Note that in the example above, the Python code spans on several lines, and you
should define a Yaml literal block using ``|``. The example above eventually
means: append ``firstname`` to ``name`` coming from the ``table1``, and put it
in the ``display_name`` cell of the target ``table1`` and ``table2``. The
target ``name`` cell will contain a copy of the source ``name`` cell.

If the target line is not supposed to have the same *id* as the source line,
you can create a new *id* with the newid() function. This function returns a
different value at each call and is responsible of incrementing the *id*. Here
is an example::

    base:
        res_users.id:
            res_users.id:
            res_users.partner_id:
            res_partner.notification_email_send: return 'comment'
            res_partner.id: |
                i = newid()
                target_rows['res_users']['partner_id'] = i
                target_rows['res_partner']['id'] = i
                target_rows['res_partner']['name'] = source_row['name']
                target_rows['res_partner']['email'] = source_row['user_email']
                return i

Each ``res_users`` line will generate a new ``res_partner`` line with a new
*id*, while the ``res_users`` *id* will be the same as the source. (Actually it
will not be the same, because an offset is applied to all ids).

Feeding a new column
--------------------

If a target column should contain data but has no equivalent in the source
table, you can use '_' as a substitute to the not existing source column name::

    base:
        res_partner._:
            res_partner.is_company: return False


Merging with existing data
--------------------------

When data is inserted in the target table, you may want to merge it with
existing data.

Imagine the target ``res_users`` table already contains an
``admin`` account, and you don't want to duplicate this account by migrating
data from the source ``res_users`` table. In this case you should tell the
mapping how to recognize existing data. This is done by replacing the
source column name with the ``__discriminator__`` statement, and by providing a
list of column names that will be used to recognize existing data::

    base:
        res_users.__discriminator__:
            - login

Using this statement, you can install a new OpenERP database with its admin
account, and merge all existing accounts with data coming from the source
table. The ``login`` column will be used to match data. The preexisting *admin*
account won't be duplicated but will be updated with the *admin* account from
the source table.

Another use case in a multicompany scenario is to merge partners existing in
the target database, but keep them separate for the two companies::

    base:
        res_partner.__discriminator__:
            - name
            - company_id

Foreign keys without constraints
--------------------------------

The first step of the migration is to automatically detect all the foreign keys
of the source and target tables. Sometimes, OpenERP defines foreign keys
without constraints. This mainly happens with *related* fields with
``store=True``, which create a column of integers without constraints. If you
don't want to ``__forget__`` such columns, you have to tell the mapping what
the target of the foreign key is, like in the real example below::

    account:
        account_move.company_id:
            account_move.company_id: __fk__ res_company


Handle cyclic dependant tables
------------------------------

During the last step, the migrated CSV files are imported one by one.  Some
tables depend on other tables through foreign key constraints, and such
dependencies sometimes happen to be cyclic. In that case, there is no way to
import tables because they all depend on another one. One solution is to
``__forget__`` the column, which is rarely desirable because you lose data. To
be able to keep such data, you should use the ``__defer__`` statement, so that
the column will be updated after all the data is imported::

    base:
        res_users.create_uid:
            res_users.create_uid: __defer__
        res_users.write_uid:
            res_users.write_uid: __defer__

Understanding errors
====================

The most difficult part of using this tool is to understand the errors during
the processing, as it requires a deep knowledge of how it internally works.
Most errors generally come from an erroneous mapping file. Errors can happen
during the processing of the CSV files, but the most difficult ones come from
the last import step, because some tables may fail to be imported. In this
case, you should carefully look at the logging messages at the end, and try to
understand the constraint errors or why tables cannot be imported. You also
should use the ``--keepcsv`` option, and inspect the intermediate CSV files to
understand the problem. By using this option, you will end up with a directory
containing five CSV files for each table.

For instance, for the ``res_partner`` table you will find these files:

 - **res_partner.csv** is the original data exported from the source
   database
 - **res_partner.target.csv** contains data after the first processing with
   the mapping file, but wrong foreign keys
 - **res_partner.target2.csv** contains final data with fixed foreign keys,
   that will eventually be imported at the end
 - **res_partner.update.csv** contains data which have been detected as
   existing in the target database, with wrong foreign keys.
 - **res_partner.update2.csv** contains the final existing data with fixed
   foreign keys, that will be used to update the target table after import.




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

