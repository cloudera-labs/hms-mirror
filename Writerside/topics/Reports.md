# Reports

Both the [Web](Web-Interface.md) and [CLI](CLI.md) interfaces generate reports.  By default, the reports are 
generated in the $HOME/.hms-mirror/reports directory with the timestamp as the 'name' of the report.  The report is 
directory of several files that include configurations, conversions, and job 'yaml' files of what was done.  Each 
database has their own reports.  The database name prefixes the report file name for the various reports.

From the CLI, you'll need to download the report directory to your local machine and use a 'Markdown' viewer to read 
it easily.

The Web interface has built in report viewing capabilities and makes it much easier to review the reports.

## Options

There are several views for the reports that include a detailed view of the process, workbooks for 'distcp', scripts for 'distcp'
, job 'yaml' files, the configuration used to run the job, etc.

You have options to view all these reports within the Web Interface. You can further 'download' a zip file of the report contents
and view them locally.

To keep things organized, there is an 'archive' option that will move the reports to an 'archive' directory.  This is useful to help
keep the reports directory clean and organized.  If you need to view those reports again, you can always move them back to the reports directory.
Although, that process is manual.

> If you run the process via the CLI and are using defaults, you'll be able to view the reports through the Web 
> Interface since they are both using the same location.