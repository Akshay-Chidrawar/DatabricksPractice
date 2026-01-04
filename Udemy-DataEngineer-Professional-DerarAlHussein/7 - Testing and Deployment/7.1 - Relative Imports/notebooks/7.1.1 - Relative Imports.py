# Databricks notebook source
# MAGIC %run ./helpers/cube_notebook

# COMMAND ----------

c1 = Cube(3)
c1.get_volume()

# COMMAND ----------

# MAGIC %md
# MAGIC ####PY file

# COMMAND ----------

from helpers.cube_notebook import Cube

# COMMAND ----------

from helpers.cube import Cube_PY

# COMMAND ----------

c2 = Cube_PY(3)
c2.get_volume()

# COMMAND ----------

# MAGIC %sh cd '/Workspace/Shared/Databricks-Certified-Data-Engineer-Professional/7 - Testing and Deployment/7.1 - Relative Imports/notebooks'

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

# MAGIC %sh ls ./helpers

# COMMAND ----------

import sys

for path in sys.path:
    print(path)

# COMMAND ----------

import os
sys.path.append(os.path.abspath('../modules'))

# COMMAND ----------

for path in sys.path:
    print(path)

# COMMAND ----------

from shapes.cube import Cube as CubeShape

# COMMAND ----------

c3 = CubeShape(3)
c3.get_volume()

# COMMAND ----------

# MAGIC %pip install ../wheels/shapes-1.0.0-py3-none-any.whl

# COMMAND ----------

from shapes_wheel.cube import Cube as Cube_WHL

# COMMAND ----------

c4 = Cube_WHL(3)
c4.get_volume()

# COMMAND ----------

#%sh pip install ../wheels/shapes-1.0.0-py3-none-any.whl
