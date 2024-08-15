# Reports

Both the [Web](Web-Interface.md) and [CLI](CLI.md) interfaces generate reports.  By default, the reports are 
generated in the $HOME/.hms-mirror/reports directory with the timestamp as the 'name' of the report.  The report is 
directory of several files that include configurations, conversions, and job 'yaml' files of what was done.  Each 
database has their own reports.  The database name prefixes the report file name for the various reports.

From the CLI, you'll need to download the report directory to your local machine and use a 'Markdown' viewer to read 
it easily.

The Web interface has built in report viewing capabilities and makes it much easier to review the reports.

> If you run the process via the CLI and are using defaults, you'll be able to view the reports through the Web 
> Interface since they are both using the same location.