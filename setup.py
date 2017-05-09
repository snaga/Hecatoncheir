from setuptools import setup, find_packages
setup(
    name="hecatoncheir",
    version="0.8",
    packages=['hecatoncheir',
              'hecatoncheir.oracle',
              'hecatoncheir.pgsql',
              'hecatoncheir.mysql',
              'hecatoncheir.mssql',
              'hecatoncheir.validator'],
#find_packages(),
    package_dir = {'hecatoncheir': 'hecatoncheir'},
    scripts=['dm-import-datamapping', 'dm-run-profiler', 'dm-export-repo',
             'dm-import-csv', 'dm-verify-results', 'dm-run-server',
             'dm-attach-file', 'dm-dump-xls'],

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires=['jinja2==2.8'],

    package_data={
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.txt', '*.rst'],
        # And include any *.msg files found in the 'hello' package, too:
        'hecatoncheir': ['templates/*/templ_*.html',
                         'templates/*/static/custom.css',
                         'templates/*/static/fonts/glyphicons/*',
                         'templates/*/static/fonts/lato/*',
                         'templates/*/static/css/flat-ui.min.css',
                         'locale/*/LC_MESSAGES/hecatoncheir.mo'
                         ],
    },

    # metadata for upload to PyPI
    author="Satoshi Nagayasu",
    author_email="snaga@uptime.jp",
    description="Open Source Data Stewardship Platform",
    license="Apache License 2.0",
    keywords="",
    url="https://github.com/snaga/hecatoncheir",   # project home page, if any

    # could also include long_description, download_url, classifiers, etc.
)
