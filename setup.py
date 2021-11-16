from setuptools import setup, find_packages

setup(
    name="seldonite", 
    packages=find_packages(),
    package_data={'seldonite': ['spark/*.json']},
    include_package_data=True
)