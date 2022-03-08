[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://GitHub.com/Naereen/StrapDown.js/graphs/commit-activity)
[![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org/)
[![Test](https://github.com/NCAS-CMS/ActiveStorage/actions/workflows/run-tests.yml/badge.svg)](https://github.com/NCAS-CMS/ActiveStorage/actions/workflows/run-tests.yml)

Active Storage (Package)
========================

**General**

First draft of the ActiveStorage Python package.

- Python: 3.9 and 3.10
- [Github Actions](https://github.com/NCAS-CMS/ActiveStorage/actions)
- Miniforge/conda-based installation

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
