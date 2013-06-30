from setuptools import setup, find_packages
import sys

version = '0.2'

if sys.version_info[0] != 2 or sys.version_info[1] != 7:
    print('This tool only works with Python 2.7')
    sys.exit(1)

setup(
    name='anybox.migration.openerp',
    version=version,
    author='Anybox',
    author_email='contact@anybox.fr',
    packages=find_packages(),
    license='GPLv3+',
    description='Fast OpenERP migration tool',
    long_description=open('README.rst').read() + open('CHANGES.rst').read(),
    url="https://bitbucket.org/anybox/anybox.migration.openerp/overview",
    include_package_data=True,
    install_requires=[
        "PyYAML",
        "psycopg2 >= 2.5",
    ],
    test_suite='anygrate.test.load_tests',
    entry_points={
        'console_scripts': [
            'migrate=anygrate.migrating:main',
        ]
    }

)
