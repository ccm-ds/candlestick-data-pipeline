#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='candlestick_data_pipeline',
      version='0.0.1',
      description='This package has shared components.',
      author='Dan Silva',
      author_email='dan.silva.1194@gmail.com',
      packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"])
    )
