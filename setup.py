#!/usr/bin/env python

from setuptools import setup, find_packages


setup(name='fito',
      version='0.1',
      description='fito',
      author='Pablo Zivic & Bruno Parrino',
      author_email='',
      url='',
      packages=find_packages('.'),
      install_requires=[
            'mmh3',
            'memoized_property'
      ],
     )
