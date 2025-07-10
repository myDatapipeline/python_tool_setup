# Engineering_Piplines_Guide
This repository container has pipeline architecture Design Pattern as a guide when considering an Engineering ETL pipeline pattern

>> python3 setup.py sdist bdist_wheel / python -m build  at the level of each repo

>> twine check dist/*

>> twine upload--repository-url https://test.pypi.org/legacy/ dist/*


project-root/
│
├── package1/
│   ├── package1/
│   │   └── __init__.py
│   ├── setup.py
│   └── pyproject.toml (optional)
│
├── package2/
│   ├── package2/
│   │   └── __init__.py
│   ├── setup.py
│   └── pyproject.toml (optional)
