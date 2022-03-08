Examples of Active Storage API
==============================

**Simple Example**

Very basic POC ``active_stroage_simple.py`` that uses a composite data loader:

```
load(filepath, var, slices, active=False, operation=None)
```

that can divert loading and application of one operation to the Active Storage, if user wants that.


**More involved Example**

Prototype example in `active_storage_more-complex.py` with the following workflow:

```
load(filepath, var, active=False, operation=None,
     internal_mask=False, external_mask=None)
```

is the loader that gets a `filepath` and a `var`: enough information to
load any data file; it does the following:

- if `active`, it loads the data and applies whatever mask AND an `operation` on the
  resulting `mask | data` and returns the result of the operation; data loading
  happens inside the Active Storage **only** for data, and on the Active Storage for mask if
  it's an *internal mask*, or outside, at Client, if *external mask*; it passes this maximum set
  of parameters to Active Storage:

  ```
  filepath: str
  var: str
  operation: str
  mask: str or Dataset (internal or external mask)
  ```

