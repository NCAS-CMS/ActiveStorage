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
