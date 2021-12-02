#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('CHANGELOG.md') as history_file:
    history = history_file.read()

requirements = [ ]
with open("requirements.txt") as requirements_file:
    requirements = requirements_file.readlines()

test_requirements = [ ]

setup(
    author="Tim Parker",
    author_email='tparker@danforthcenter.org',
    python_requires='>=3.8',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
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
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/tparkerd/pgwasdbi',
    version='0.1.0',
    zip_safe=False,
)
