from setuptools import setup
import glob

setup(
    name='pysgcn',
    version='0.0.2',
    description='Data building codes for the synthesized SGCN database',
    url='http://github.com/usgs-bcb/pysgcn',
    author='R. Sky Bristol',
    author_email='sbristol@usgs.gov',
    license='unlicense',
    packages=['pysgcn'],
    package_data={'pysgcn': ['resources/sgcn_source_records_schema.json']},
    install_requires=[
        'sciencebasepy',
        'pandas',
        'requests'
    ],
    zip_safe=False
)
