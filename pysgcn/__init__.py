# pysgcn package

import pkg_resources

from . import sgcn

__version__ = pkg_resources.require("pysgcn")[0].version


def get_package_metadata():
    d = pkg_resources.get_distribution('pysgcn')
    for i in d._get_metadata(d.PKG_INFO):
        print(i)

