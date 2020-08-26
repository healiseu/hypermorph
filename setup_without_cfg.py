import fnmatch
from setuptools import find_packages, setup
from setuptools.command.build_py import build_py as build_py_orig

exclude = ['']

class build_py(build_py_orig):

    def find_package_modules(self, package, package_dir):
        modules = super().find_package_modules(package, package_dir)
        return [(pkg, mod, file, ) for (pkg, mod, file, ) in modules
                if not any(fnmatch.fnmatchcase(pkg + '.' + mod, pat=pattern)
                for pattern in exclude)]

setup(
    name="hypermorph",
    version="0.9.0",
    author="Athanassios I. Hatzis",
    author_email="hatzis@healis.eu",
    description="TRIADB - Self-Service Data Management and Analytics Framework",
    long_description="TRIADB - Self-Service Data Management and Analytics Framework",
    license="Default license AGPL and TriaClick Open Source License Agreement",
    url="http://healis.eu/triadb",
    packages=find_packages(),
    cmdclass={'build_py': build_py},
    platforms=['Linux'],
    classifiers=[
	"Development Status :: 5 - Production/Stable",
	"Environment :: Console",
	"Environment :: Web Environment",
	"Framework :: IPython",
	"Framework :: Jupyter",
	"Framework :: Flask",
	"Intended Audience :: Developers",
	"Intended Audience :: End Users/Desktop",
	"License :: Free For Home Use",
	"License :: Free for non-commercial use",
	"License :: Freely Distributable",
	"License :: Free To Use But Restricted",
	"License :: OSI Approved :: GNU Affero General Public License v3",
	"Operating System :: POSIX :: Linux",
	"Programming Language :: Python :: 3.7",
	"Topic :: Software Development :: Libraries :: Application Frameworks",
	"Topic :: Database :: Database Engines/Servers",
	"Topic :: Database :: Front-Ends",	
	"Topic :: Scientific/Engineering :: Information Analysis",
	"Topic :: Scientific/Engineering :: Visualization"
    ]
)
