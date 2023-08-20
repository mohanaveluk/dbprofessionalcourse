# Databricks notebook source
# MAGIC %run ../includes/copy_dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace view customer_vw as
# MAGIC select 
# MAGIC   customer_id,
# MAGIC   case 
# MAGIC     when is_member('admins_demo') then email
# MAGIC     else 'REDACTED'
# MAGIC   end as email,
# MAGIC   gender,
# MAGIC   case 
# MAGIC     when is_member('admins_demo') then first_name
# MAGIC     else 'REDACTED'
# MAGIC   end as first_name,
# MAGIC
# MAGIC   case 
# MAGIC     when is_member('admins_demo') then last_name
# MAGIC     else 'REDACTED'
# MAGIC   end as last_name,
# MAGIC
# MAGIC   case 
# MAGIC     when is_member('admins_demo') then street
# MAGIC     else 'REDACTED'
# MAGIC   end as street,
# MAGIC   city,
# MAGIC   country,
# MAGIC   row_status,
# MAGIC   row_time
# MAGIC from 
# MAGIC customers_silver 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace view customer_fr_vw as 
# MAGIC select * from customer_vw
# MAGIC where 
# MAGIC   case
# MAGIC     when is_member('admins_demo') then true
# MAGIC     else country = 'France' AND row_time > '2022-01-01'
# MAGIC   end
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from customer_fr_vw
