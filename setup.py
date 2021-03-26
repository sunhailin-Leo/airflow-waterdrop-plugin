import os
import sys
import pathlib
from shutil import rmtree

from setuptools import Command, find_packages, setup

# RELEASE STEPS
# $ python setup.py upload

__title__ = "airflow_waterdrop_plugin"
__description__ = "A FastAPI Middleware of Apollo(Config Server By CtripCorp) " \
                  "to get server config in every request."
__url__ = "https://github.com/sunhailin-Leo/airflow-waterdrop-plugin"
__author_email__ = "379978424@qq.com"
__license__ = "MIT"

here = pathlib.Path(__file__).parent.absolute()
with open(here / "requirements.txt") as requirements_fife:
    __requires__ = [
        requirement_line.rstrip("\n")
        for requirement_line in requirements_fife
    ]
__keywords__ = ["airflow", "waterdrop"]

# Load the package's _version.py module as a dictionary.
about = {}
with open(os.path.join(here, __title__, "_version.py")) as f:
    exec(f.read(), about)


__version__ = about["__version__"]


class UploadCommand(Command):
    description = "Build and publish the package."
    user_options = []

    @staticmethod
    def status(s):
        print("✨✨ {0}".format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status("Removing previous builds…")
            rmtree(os.path.join(here, "dist"))
            rmtree(os.path.join(here, "build"))
            rmtree(os.path.join(here, "{0}.egg-info".format(__title__)))
        except OSError:
            pass

        self.status("Building Source and Wheel distribution…")
        os.system("{0} setup.py bdist_wheel".format(sys.executable))

        self.status("Uploading the package to PyPI via Twine…")
        os.system("twine upload dist/*")

        self.status("Pushing git tags…")
        os.system('git tag -a v{0} -m "release version v{0}"'.format(__version__))
        os.system("git push origin v{0}".format(__version__))

        sys.exit()


setup(
    name=__title__,
    version=__version__,
    description=__description__,
    url=__url__,
    author=about["__author__"],
    author_email=__author_email__,
    license=__license__,
    packages=find_packages(exclude=("test",)),
    keywords=__keywords__,
    install_requires=__requires__,
    zip_safe=False,
    include_package_data=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries",
    ],
    cmdclass={"upload": UploadCommand},
    # extras_require=__extra_requires__,
)
