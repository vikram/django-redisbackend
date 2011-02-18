from setuptools import setup, find_packages

DESCRIPTION = 'Adds Redis Cache to the table'
LONG_DESCRIPTION = None
try:
    LONG_DESCRIPTION = open('README.rst').read()
except:
    pass

setup(name='django-redisbackend',
      version='0.1',
      packages=find_packages(),
      author='Vikram Bhandoh',
      author_email='vikram.bhandoh@gmail.com',
      url='http://github.com/vikram/django-redisbackend',
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      platforms=['any'],
      classifiers=[
          'Development Status :: 1 - Alpha',
          'Environment :: Web Environment',
          'Framework :: Django',
          'Intended Audience :: Developers',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Topic :: Software Development :: Libraries :: Application Frameworks',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'License :: OSI Approved :: BSD License',
      ],
)
