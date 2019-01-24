#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

# To update the package version number, edit nse_project/__version__.py
version = {}
with open(os.path.join(here, 'nse_project', '__version__.py')) as f:
    exec(f.read(), version)

with open('README.rst') as readme_file:
    readme = readme_file.read()

setup(
    name='nse_project',
    version=version['__version__'],
    description="An exercise to pull Natonal Stock Exchange Data a(India) for stock market trend analysis",
    long_description=readme + '\n\n',
    author="Rajkumar Durairaj",
    author_email='rajkumar.durai@gmail.com',
    url='https://github.com/rajkumar-d83/nse_project',
    packages=[
        'nse_project',
    ],
    package_dir={'nse_project':
                 'nse_project'},
    include_package_data=True,
    license="GNU General Public License v3 or later",
    zip_safe=False,
    keywords='nse_project',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    test_suite='tests',
    install_requires=[],  # FIXME: add your package's dependencies to this list
    setup_requires=[
        # dependency for `python setup.py test`
        'pytest-runner',
        # dependencies for `python setup.py build_sphinx`
        'sphinx',
        'sphinx_rtd_theme',
        'recommonmark'
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
        'pycodestyle',
    ],
    extras_require={
        'dev':  ['prospector[with_pyroma]', 'yapf', 'isort'],
    }
)
