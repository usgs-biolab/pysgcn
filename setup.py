from setuptools import setup

setup(
    name='pysgcn',
    version='0.0.1',
    description='Data building codes for the synthesized SGCN database',
    url='http://github.com/usgs-bcb/pysgcn',
    author='R. Sky Bristol',
    author_email='sbristol@usgs.gov',
    license='unlicense',
    packages=['pysgcn'],
    install_requires=[
        'sciencebasepy',
        'pandas',
        'requests'
    ],
    zip_safe=False
)
