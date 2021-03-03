## Output

### Application Report

When the process has completed a markdown report is generated with all the work that was done.  The report location will be at the bottom of the output window.  You will need a *markdown* renderer to read the report.  Markdown is a textfile, so if you don't have a renderer you can still look at the report, it will just be harder to read.

The report location is displayed at the bottom on the application window when it's complete.

### Action Report for LEFT and RIGHT Clusters

Under certain conditions, additional actions may be required to achieve the desired 'mirror' state.  An output script for each cluster is created with these actions.  These actions are NOT run by `hms-mirror`.  They should be reviewed and understood by the owner of the dataset being `mirrored` and run when appropriate.

The locations are displayed at the bottom on the application window when it's complete.

### SQL Execution Output

A SQL script of all the actions taken will be written to the local output directory.

## Logs

There is a running log of the process in `$HOME/.hms-mirror/logs/hms-mirror.log`.  Review this for details or issues of the running process.


