#! /usr/bin/env python
# Authors: Olivier Grisel <olivier.grisel@ensta.org>
#          MinRK
# LICENSE: Simple BSD


from distutils.core import setup

setup(
    name="pyrallel",
    version="0.1.0",
    description="Experimental tools for parallel machine learning",
    author="Olivier Grisel",
    author_email="olivier.grisel@ensta.org",
    url='http://github.com/pydata/pyrallel',
    packages=[
        'pyrallel',
    ],
    # TODO: convert README from markdown to rst
    # long_description=open('README.rst').read(),
)
