#! /usr/bin/env python
# Authors: Olivier Grisel <olivier.grisel@ensta.org>
#          MinRK
# LICENSE: MIT
from distutils.core import setup

setup(
    name="pyrallel",
    version="0.2",
    description="Experimental tools for parallel machine learning",
    maintainer="Olivier Grisel",
    mainainer_email="olivier.grisel@ensta.org",
    license="MIT",
    url='http://github.com/pydata/pyrallel',
    packages=[
        'pyrallel',
    ],
    classifiers=[
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved',
        'Programming Language :: Python',
        'Topic :: Software Development',
        'Topic :: Scientific/Engineering',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Operating System :: MacOS',
        'Development Status :: 3 - Alpha',
    ],
    # TODO: convert README from markdown to rst
    # long_description=open('README.rst').read(),
)
