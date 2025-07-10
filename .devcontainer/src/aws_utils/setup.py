from setuptools import setup, find_packages

setup(
    name='aws_utils',
    version='0.1.0',
    packages=find_packages(
    where='src',  # '.' by default
        include=['package1'],  # ['*'] by default
        exclude=['tests'],  # empty by default
    ),
    description='Package 1 - separate distribution',
    author='Gopherwood Consulting',
)
