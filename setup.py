#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.md') as history_file:
    history = history_file.read()

requirements = [ 'pandas', 'psycopg2-binary', 'configparser', 'tqdm', 'python-dotenv', 'pandas_schema', 'asyncpg' ]

setup_requirements = [ ]

test_requirements = [ ]

setup(
    author="Tim Parker",
    author_email='Tim.ParkerD@gmail.com',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
    ],
    description="Data importation modules for Baxter lab's GWAS database",
    entry_points={
        'console_scripts': [
            'pgwasdbi=pgwasdbi.cli:main',
        ],
    },
    install_requires=requirements,
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='pgwasdbi',
    name='pgwasdbi',
    packages=find_packages(include=['pgwasdbi', 'pgwasdbi.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/tparkerd/pgwasdbi',
    version='0.2.0',
    zip_safe=False,
)
