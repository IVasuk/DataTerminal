from setuptools import setup
import sys,os

setup(
    name = 'data-terminal',
    version = '0.1.0',
    description = 'Data terminal',
    license='GPL v3',
    author = 'vasuk.mobile@gmail.com',
    packages = ['src'],
    package_data={'src': ['description.txt','ui/*']
                 },
    install_requires=['future','pycairo','pygobject'],
#    scripts=["src/wrappers/data-terminal-wrapper","src/scripts/management-script"],

    entry_points = {
        'gui_scripts': [
            'data-terminal=src.main:main']
            },
    classifiers = ['Operating System :: OS Independent',
            'Programming Language :: Python :: 3.8',
            'Operating System :: MacOS :: MacOS X',
            'Operating System :: Microsoft :: Windows',
            'Operating System :: POSIX',
            'License :: OSI Approved :: GNU General Public License v3 (GPLv3)'],
)
