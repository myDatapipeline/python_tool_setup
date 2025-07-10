from setuptools import setup, find_packages  # or find_namespace_packages
from os import path

work_directory =path.abspath(path.dirname(__file__))

with open(path.join(work_directory,'README.md'),encoding='utf-8') as f :
    long_description  =f.read()

setup(
    name ='my_test_setup_package',
    version='0.0.1',
    author='Gopherwood Consulting',
    author_email='contact-us@gopherwood.org',
    description ='Simple test package',
    long_description= long_description,
    long_description_content_type= 'text/markdown',
    # ...
    packages=find_packages(
        where='src',  # '.' by default
        include=['package','package2'],  # ['*'] by default
        exclude=['tests'],  # empty by default
    ),
    # ...
    install_requires =[]
)