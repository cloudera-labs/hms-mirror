# Tips for Running `hms-miror`

## Run in `screen` or `tmux`

This process can be a long running process.  It depends on how much you've asked it to do.  Having the application terminated because the `ssh` session to the edgenode timed out and you're computer went to sleep will be very disruptive.

Using either of these session state tools (or another of your choice) while running it on an edgenode will allow you to sign-off without disrupting the process AND reattach to see the interactive progress at a later point.

## Use `dryrun` FIRST

Before you go and run a process the will make changes, try running `hms-mirror` with the `dryrun` option first.  The report generated at the end of the job will provide insight into what issues (if any) you'll run across.

For example: When running the either the METADATA or STORAGE phase and setting `overwriteTable: "true"` in the configuration, the process will `ERROR` if the table exists in the RIGHT cluster AND it manages the data there.  `hms-mirror` will not `DROP` tables that manage data, you'll need to do that manually.  But the processes leading upto this check can be time consuming and a failure late in the process is annoying.


## Start Small

Use `-db`(database) AND `-tf`(table filter) options to limit the scope of what your processing.  Start with a test database that contains various table types you'd like to migrate.

Review the output report for details/problems you encountered in processing.

Since the process expects the user to have adequate permissions in both clusters, you may find you have some prework to do before continuing.

## RETRY (WIP-NOT FULLY IMPLEMENTED YET)

When you run `hms-mirror` we write a `retry` file out to the `$HOME/.hms-mirror/retry` directory with the same filename prefix as the config used to run the job.  If you didn't specify a config then its using the `$HOME/.hms-mirror/cfg/default.yaml` file as a default.  This results in a `$HOME/.hms-mirror/retry/default.retry` file.

If the job fails part way through you can rerun the process with the `--retry` option and it will load the _retry_ file and retry any process that wasn't previously successful.