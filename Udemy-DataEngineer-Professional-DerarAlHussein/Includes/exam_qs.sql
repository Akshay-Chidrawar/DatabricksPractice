-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####Test1 - Q41 describe detail only shows table comments, describe extended will show column comments also.

-- COMMAND ----------

create table test2 (eid int ,ename varchar(20))
comment 'test table';
alter table test2
add constraint eid_check check (eid>0);
alter table test2
alter column eid comment 'positive number';

-- COMMAND ----------

describe test2;

-- COMMAND ----------

describe extended test2;

-- COMMAND ----------

describe detail test2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Does RENAME command appears or not in DESCRIBE HISTORY

-- COMMAND ----------

create table test (eid int, ename varchar(20));
insert into test values (1,'A');
insert into test values (2,'B');
insert into test values (3,'C');
describe history test;

-- COMMAND ----------

-- DBTITLE 1,one time
--%python
--spark.conf.set ("spark.databricks.delta.alterTable.rename.enabledOnAWS","True");

-- COMMAND ----------

alter table test rename to newtest;
delete from newtest;
describe history newtest;

-- COMMAND ----------

drop table newtest;
drop table test;

-- COMMAND ----------

select * from json.`dbfs:/user/hive/warehouse/newtest/_delta_log/00000000000000000000.json`;

-- COMMAND ----------

select * from json.`dbfs:/user/hive/warehouse/newtest/_delta_log/00000000000000000001.json`;

-- COMMAND ----------

select * from json.`dbfs:/user/hive/warehouse/newtest/_delta_log/00000000000000000002.json`;
