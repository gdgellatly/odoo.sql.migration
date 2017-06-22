from setuptools import setup, find_packages
import sys

version = '0.11'

if sys.version_info[0] != 2 or sys.version_info[1] != 7:
    print('This tool only works with Python 2.7')
    sys.exit(1)

setup(
    name='odoo.sql.migration',
    version=version,
    author='O4SB',
    author_email='g@o4sb.com',
    packages=find_packages(),
    license='GPLv3+',
    description='Fast OpenERP migration framework',
    long_description=open('README.rst').read() + open('CHANGES.rst').read(),
    url="https://github.com/gdgellatly/odoo.sql.migration",
    include_package_data=True,
    install_requires=[
        "PyYAML",
        "psycopg2 >= 2.5",
        "toolz",
    ],
    test_suite='migration.test.load_tests',
    entry_points={
        'console_scripts': [
            'migrate=migration.migrating:main',
        ]
    }

)
