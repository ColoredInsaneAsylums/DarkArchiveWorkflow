from distutils.core import setup

with open("PACKAGE_VERSION", "r") as f:
      ver = f.readlines()[0].rstrip()

setup(name='metadatautilspkg',
      version=ver,
      description='Metadata utilities for archiving digital objects',
      author='Nitin Verma',
      author_email='nitin.verma@utexas.edu',
      url='https://wikis.utexas.edu/display/CSHProject/CSHProject+Home',
      packages=['metadatautilspkg'],
     )
