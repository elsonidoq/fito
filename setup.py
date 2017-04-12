#!/usr/bin/env python

from setuptools import setup, find_packages
import fito

setup(
    name='fito',
    packages=find_packages(),
    version=fito.__version__,
    description='fito',
    author='Pablo Zivic',
    author_email='elsonidoq@gmail.com',
    url='https://github.com/elsonidoq/fito',
    download_url='https://github.com/elsonidoq/fito/tarball/' + fito.__version__,
    zip_safe=False,
    install_requires=[
        'mmh3',
        'memoized_property',
        'PyYAML',
        'cmd2'
    ],
)
