from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='VaingloriousEye',
      version=version,
      description="WSGI middleware that does live logging of requests",
      long_description="""\
""",
      classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Framework :: Paste",
        "License :: OSI Approved :: MIT License",
        "Topic :: Internet :: WWW/HTTP :: WSGI",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Middleware",
      ],
      keywords='wsgi web log egoism',
      author='Ian Bicking',
      author_email='ianb@colorstudy.com',
      url='http://pythonpaste.org/vaingloriouseye/',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
        "Paste",
        "WebOb",
        "SQLAlchemy",
        "WaitForIt",
        ],
      tests_require=[
        "WebTest",
        "nose",
        ],
      test_suite='nose.collector',
      entry_points="""
      [console_scripts]
      import-vaineye = vaineye.importer:main
      
      [paste.filter_app_factory]
      main = vaineye.statuswatch:make_status_watcher

      [paste.app_factory]
      stats = vaineye.view:make_vaineye_view
      """,
      )
      
