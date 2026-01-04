**Azure**

1. Create a storage account.

2. Inside above storage account, create 3 separate containers to store Metastore, managed datasets, unmanaged datasets.

3. Create a Databricks Access Connector. Whichever Azure services you want Databricks to be able to access (e.g. Storage account), link all those services with this Databricks Access Connector.


**Databricks**

**Container1** -->
1. Data tab > Create a metastore; mention the Databricks Access Connector as a middleman between Databricks and Azure; this now creates a storage credential for Azure storage account in Databricks environment. Then provide the link to appropriate container (Storage account name + Container name).

2. Catalog Explorer > Create 2 external locations (separately) for below containers using same above storage credential from dropdown menu:


**Container2** --> Create catalog "dev" in this location.

**Container3** -->
1. Unmanaged data (data zone) - include directory "data" in url.
2. Checkpoints (checkpoints) - include directory "checkpoint" in url.

Create user group "developers" and assign appropriate users to the group. Grant full access to "developers" user group to below external locations:

1. Catalog

2. Unmanaged data

3. Checkpoints
