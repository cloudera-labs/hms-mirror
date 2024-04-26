# Runbook for database: sm_orders

You'll find the **run report** in the file:

`conversion/literal-dc/sm_orders_hms-mirror.md|html` 

This file includes details about the configuration at the time this was run and the output/actions on each table in the database that was included.

## Steps

Execute was **OFF**.  All actions will need to be run manually. See below steps.

1. **LEFT** clusters SQL script. (Has NOT been executed yet)
2. **LEFT** cluster `distcp` actions.  Needs to be performed manually.  Use 'distcp' report/template.
3. **LEFT** clusters CLEANUP SQL script. (Has NOT been executed yet)
