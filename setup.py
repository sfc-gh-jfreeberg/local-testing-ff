"""
The Conda environment.yml will use this to create an editable installation
of this project.
"""

from setuptools import setup, find_packages

setup(
    name="Example Snowpark Python project",
    version="0.1.0",
    packages=find_packages()
)
