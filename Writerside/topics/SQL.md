# SQL

The **SQL** data strategy will use Hive SQL to move data between clusters.  When the cluster don't have direct line of sight to each other and can NOT be [linked](Linking-Cluster-Storage-Layers.md), you can use options like `-cs` or `-is` to bridge the gap.

![sql](images/sql_exp-imp.png)
