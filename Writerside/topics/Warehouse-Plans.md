# Warehouse Plans

Warehouse Plans are the way you can control how 'each' Database is translated between clusters or storage environments.
Warehouse plans allow you to control where the data will be translated to.

There are three types of 'Warehouse Plans'.

- Global
- Per Database
- Environment

When you choose to 'ALIGN' your datasets during the migration (See [Location Alignment](Location-Alignment.md) for
settings that are applicable for each strategy), we'll evaluate the warehouse plans to determine where each dataset
should land.  If you're using 'SQL' to move data, we'll build the schema's and sql that will make the adjustments
required to reorganize the data based on the warehouse plan that is found.

It's recommended to define a warehouse plan for each **database** you want to move when you're using the 'ALIGNED'
data movement strategy.  When this is defined for the database, we'll inspect the all the current locations of tables
and partitions in that dataset and make the necessary adjustments to locations.

The 'Warehouse Plans' get converted into 'Global Location Maps' that are inspected during processing to make that conversion.

## Standard Locations

When you choose to **ALIGN** the datasets, you are choosing to collect everything in the dataset/database under the same
location as you've defined in the **warehouse plan**.  When you choose `DISTCP` as the movement plan for `ALIGNED`, we'll
build a distcp plan that will make those translations.  **BUT**, there are some restrictions to this.

When you choose to use non-standard locations for 'partition specs', we can't build a proper `distcp` plan.  In this case
we will throw an 'error' for the offending table and describe to imbalance.  You can either fix/adjust the dataset OR choose
to use the `SQL` data movement strategy.

