# Databricks notebook source
# MAGIC %run ./helpers/cube

# COMMAND ----------

c1 = Cube(5)
#c1.get_volume()
c1.get_surface_area()


# COMMAND ----------

from helpers.cube_f import Cube_PY

# COMMAND ----------

c1 = Cube_PY(5)
c1.get_volume()
#c1.get_surface_area()


# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

import sys
for path in sys.path:
    print(path)

# COMMAND ----------


import os
sys.path.append(os.path.abspath('../modules'))

# COMMAND ----------

from shapes.cube import Cube as cubeShape

# COMMAND ----------

c2 = cubeShape(4)
c2.get_volume()
