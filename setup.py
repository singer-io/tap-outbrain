#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name="tap-outbrain",
      version="2.0.0",
      description="Singer.io tap for extracting data from the Outbrain API",
      author="Fishtown Analytics",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_outbrain"],
      install_requires=[
          "singer-python==6.8.0",
          "backoff==2.2.1",
          "requests==2.34.2",
          "python-dateutil==2.9.0.post0"
      ],
      extras_require = {
        "dev": [
          "pytest",
        ],
      },
      entry_points="""
          [console_scripts]
          tap-outbrain=tap_outbrain:main
      """,
      packages=find_packages(),
      include_package_data=True,
      package_data={
          "tap_outbrain": ["schemas/*.json"],
      }
)
