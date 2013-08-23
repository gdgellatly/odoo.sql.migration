Changes
=======

0.6 (unreleased)
----------------

- Migrate ir_sequence without needing post-migration script
- Fixed workflow instance and workitem migration
- Major performance improvement (x3) in case of db merging
- Break some dependency loops and other mapping improvements

0.5 (2013-08-02)
----------------

- Fixed foreign keys pointing to a ``__moved__`` table with existing data
- Updated doc

0.4 (2013-07-28)
----------------

- Fixed migration of leads and purchase orders
- simplified ``__moved__`` statement handling
- improved workflow migration
- migrate employees and expenses
- set suppliers as companies by default
- how to install in a buildout
- updated doc

0.3 (2013-07-11)
----------------

- Lots of improvements for the 6.1 to 7.0 migration
- Fixed a bug during import due to bad quoting
- Allow m2o to m2m migration without custom code
- Added mapping for project, crm and auth_ldap modules
- Fixed move lines
- Allow to request the source db as well
- Improved documentation
- Migration of running workflows


0.2 (2013-07-01)
----------------

 - initial release
