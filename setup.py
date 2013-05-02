from setuptools import setup, find_packages

setup(
    name='anybox.migration.openerp',
    version='0.1',
    author='Florent JOUATTE',
    author_email='fjouatte@anybox.fr',
    packages=find_packages(),
    license='LICENSE.txt',
    description='Resolving dependencies tree',
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
