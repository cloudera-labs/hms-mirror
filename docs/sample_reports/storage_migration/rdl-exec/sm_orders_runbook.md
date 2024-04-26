# Runbook for database: sm_orders

You'll find the **run report** in the file:

`conversion/rdl-exec/sm_orders_hms-mirror.md|html` 

This file includes details about the configuration at the time this was run and the output/actions on each table in the database that was included.

## Steps

Execute was **ON**, so many of the scripts have been run already.  Verify status in the above report.  `distcp` actions (if requested/applicable) need to be run manually. Some cleanup scripts may have been run if no `distcp` actions were requested.

1. **LEFT** clusters SQL script.  (Has been executed already, check report file details)
2. **LEFT** clusters CLEANUP SQL script. (Has NOT been executed yet)
