Python PySpark Training Repository
==============
**Author:** *Yûki VACHOT*

**Updated:** **10/01/24**
# CONTENT TABLE


---
# Installation

`python -m venv `

- [Python 3.11.7](https://www.python.org/downloads/)
- [Spark 3.5.0 with Hadoop 3.0.0](https://spark.apache.org/downloads.html)
- [winutils.exe, .pdb and hadoop.dll](https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin)
- [Java JDK 17](https://www.azul.com/downloads/?version=java-17-lts&package=jdk#zulu)
- [pygraphviz](https://pygraphviz.github.io/documentation/stable/install.html#windows) install in x86
  - `pip install --global-option=build_ext --global-option="-IC:\Program Files (x86)\Graphviz\include" --global-option="-LC:\Program Files (x86)\Graphviz\lib" pygraphviz`
---
# Run Python PySpark
- `python init.py`
---
# Run Python Test
- path from src/test_pyspark_training
- `pytest -k test_`
---
# Run pylint for code check

---
# Run Python doc with Sphinx

---