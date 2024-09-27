# Logs

Application Logs are a detailed record of the events that have occured in the application. Logs are useful for debugging and troubleshooting issues. 

Logs are stored in the `$HOME/.hms-mirror/logs` directory.  Unless the user has specified a different **output 
directory** when running the `CLI` interface.  When the `-o <directory>` option is used, the logs will be stored in 
the output directory along with the job files and reports.

For the `--service` or WebUI version of `hms-mirror`, the logs are stored in the `$HOME/.hms-mirror/logs` directory 
as `hms-mirror-service.log`.  This log file will rotate every 100Mb and moved to the `$HOME/.
hms-mirror/logs/archived` directory.  The log file will be named with the timestamp of when the log was rotated.
