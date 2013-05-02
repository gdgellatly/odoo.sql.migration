from setuptools import setup, find_packages

setup(
    name='anybox.migration.openerp',
    version='0.1',
    author='Anybox',
    author_email='contact@anybox.fr',
    packages=find_packages(),
    license='GPLv3+',
    description='Quick OpenERP migration tool',
    long_description=open('README.rst').read() + open('CHANGES.rst').read(),
    url="https://bitbucket.org/anybox/anybox.migration.openerp/overview",
    install_requires=[
        "openobject-library < 2.0",
        "PyYAML",
        "psycopg2 >= 2.5",
    ],
    test_suite='anygrate.test.load_tests',
    entry_points={
        'console_scripts': [
            'migrate=anygrate.migrating:main',
            'order=anygrate.depending:main',
        ]
    }

)
