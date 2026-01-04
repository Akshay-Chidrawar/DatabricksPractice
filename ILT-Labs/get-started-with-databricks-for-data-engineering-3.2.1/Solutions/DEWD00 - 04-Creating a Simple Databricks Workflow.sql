-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Get Started with Databricks for Data Engineering
-- MAGIC ## Demo 04 - Creating a Simple Databricks Workflow
-- MAGIC
-- MAGIC Databricks Workflows provides a collection of tools that allow you to schedule and orchestrate data processing tasks on Databricks. You use Databricks Workflows to configure Databricks Jobs.
-- MAGIC
-- MAGIC **Objective:** Use the pipeline built in the previous demonstration to create two tasks in a job. The pipeline has been separated into two notebooks for demonstration purposes:
-- MAGIC - **DEWD00 - 04A-Workflow Task 1 - Setup - Bronze**
-- MAGIC - **DEWD00 - 04B-Workflow Task 2 - Silver - Gold**
-- MAGIC
-- MAGIC
-- MAGIC **NOTE:** You could have used a Delta Live Table (DLT) pipeline for this data engineering task, but DLT is beyond the scope of this course. DLTs can be scheduled within a Workflow with additional tasks.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup (required)
-- MAGIC To set up your lab environment, execute the following scripts. Be sure to review the environment information provided, and take note of the module's catalog name (**getstarted**) and your unique schema name.

-- COMMAND ----------

-- MAGIC %run ./setup/04_demo_setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1. Generate Job Configuration
-- MAGIC
-- MAGIC Configuring this job will require parameters unique to a given user.
-- MAGIC
-- MAGIC Run the cell below to print out values you'll use to configure your job in subsequent steps.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC learner.print_workflow_job_info()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. Configure Job with a Notebook Task
-- MAGIC
-- MAGIC When using the Jobs UI to orchestrate a workload with multiple tasks, you'll always begin by creating a job with a single task.
-- MAGIC
-- MAGIC Complete the following to create a job with two tasks:
-- MAGIC 1. Right-click the **Workflows** button on the sidebar, and open the link in a new tab. This way, you can refer to these instructions, as needed.
-- MAGIC 2. Click the **Jobs** tab, and click the **Create Job** button.
-- MAGIC 3. In the top-left of the screen, enter the **Job Name** provided above to add a name for the job.
-- MAGIC 4. Configure the job and task as specified below. You'll need the values provided in the cell output above for this step.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ### Create Task 1
-- MAGIC | Setting | Instructions |
-- MAGIC |--|--|
-- MAGIC | Task name | Enter **Setup-Bronze** |
-- MAGIC | Type | Choose **Notebook**. Note in the dropdown list the many different types of jobs that can be scheduled |
-- MAGIC | Source | Choose **Workspace** |
-- MAGIC | Path | Use the navigator to specify the **DEWD00 - 04A-Workflow Task 1 - Setup - Bronze** notebook. Use the path from above to help find the notebook. |
-- MAGIC | Cluster | From the dropdown menu, under **Existing All Purpose Clusters**, select your cluster |
-- MAGIC | Create | Select the **Create task** button to create the task |
-- MAGIC
-- MAGIC **NOTE**: When selecting your all-purpose cluster, you may get a warning about how this will be billed as all-purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## 3. Explore Scheduling Options
-- MAGIC Complete the following steps to explore the scheduling options:
-- MAGIC 1. On the right hand side of the Jobs UI, locate the **Schedules & Triggers** section.
-- MAGIC 2. Select the **Add trigger** button to explore scheduling options.
-- MAGIC 3. Changing the **Trigger type** from **None (Manual)** to **Scheduled** will bring up a cron scheduling UI.
-- MAGIC    - This UI provides extensive options for setting up chronological scheduling of your Jobs. Settings configured with the UI can also be output in cron syntax, which can be edited if custom configuration is not available when the UI is needed.
-- MAGIC 4. Select **Cancel** to return to Job details.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Run Job
-- MAGIC Select **Run now** above  **Job details** to execute the job.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Review Job Run
-- MAGIC
-- MAGIC To review the job run:
-- MAGIC 1. On the Job details page, select the **Runs** tab in the top-left of the screen (you should currently be on the **Tasks** tab)
-- MAGIC 2. Find your job.
-- MAGIC 3. Open the output details by clicking on the timestamp field under the **Start time** column
-- MAGIC     - If **the job is still running**, you will see the active state of the notebook with a **Status** of **`Pending`** or **`Running`** in the right side panel. 
-- MAGIC     - If **the job has completed**, you will see the full execution of the notebook with a **Status** of **`Succeeded`** or **`Failed`** in the right side panel

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cleanup
-- MAGIC Drop the tables created in the job.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drop_tables = ['current_employees_bronze', 'current_employees_silver', 'total_roles_gold']
-- MAGIC for table in drop_tables:
-- MAGIC     spark.sql(f'DROP TABLE IF EXISTS {learner.catalog_name}.{learner.my_schema}.{table}')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
