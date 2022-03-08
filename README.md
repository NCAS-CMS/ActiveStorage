[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://GitHub.com/Naereen/StrapDown.js/graphs/commit-activity)
[![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org/)
![Github Actions](https://github.com/github/docs/actions/workflows/run-tests.yml/badge.svg)

Active Storage (Package)
========================

**General**

First draft of the ActiveStorage package.

**Create `conda` environment**

Use `environment.yml` to create a virtual environment:
```
mamba env create -n activestorage -f environment.yml
conda activate activestorage
```

**Install package plus PyPi dependencies for tests with flake, lint...**

```
pip install -e .[develop]
```
