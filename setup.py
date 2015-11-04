from setuptools import setup

setup(name='tc-coalesce',
      version='0.1.0',
      description='A task coalescing service for Taskcluster',
      author='Jake Watkins',
      author_email='jwatkins@mozilla.com',
      url='https://github.com/dividehex/tc-coalesce',
      py_modules=['tc-coalesce'],
      license='MPL2',
      extras_require={
          'test': [
              'nose',
              'mock',
              'pep8',
              'pyflakes',
              'coverage',
          ]
      })
