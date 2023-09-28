""" Usual setup file for package """
# read the contents of your README file
from pathlib import Path

from setuptools import find_packages, setup

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="perfcapture",
    version="0.0.1",
    license="MIT",
    description="Capture the performance of a computer system whilst running a set of benchmark workloads.",
    author="Jack Kelly",
    author_email="info@openclimatefix.org",
    company="Open Climate Fix Ltd",
    install_requires=[],  # TODO
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    scripts=['scripts/perfcapture.py'],
)
