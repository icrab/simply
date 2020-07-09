#!/usr/bin/env python

from distutils.core import setup

setup(name='simply',
      version='0.1',
      description='Simple RPC system for the great good',
      author='Sergey Sosnin',
      author_email='serg.sosnin@gmail.com',
      packages=['simply'],
      install_requires=[
          'msgpack',
          'pebble',
          'redis',
          'envs'
      ]
     )