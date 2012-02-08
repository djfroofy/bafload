#!/usr/bin/env python

from setuptools import setup, find_packages

from bafload import version as VERSION

PKGNAME = 'bafload'

exclude = []
def load_requirements(requirements):
    fd = open(requirements)
    entries = []
    for line in fd:
        entry = line.strip()
        if not entry:
            continue
        if entry[0] in ('-', '#'):
            continue
        dep = entry
        if '==' in entry:
            dep, v = entry.split('==')
        if dep in exclude:
            continue
        entries.append(entry)
    fd.close()
    return entries

requirements = load_requirements('requirements.txt')

packages = [PKGNAME] + [ ( '%s.%s' % (PKGNAME, pkg) ) for pkg in find_packages(PKGNAME) ]

setup(
    name='Bafload',
    author = "Drew Smathers",
    version=VERSION,
    description='A Big Ass File (Up/Down)loader that works with s3',
    packages=packages,
    install_requires=requirements,
    zip_safe=False,
)

