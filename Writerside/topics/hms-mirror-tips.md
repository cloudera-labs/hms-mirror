# Tips

<tip>
Run in `screen` or `tmux`

This process can be a long-running process.  It depends on how much you've asked it to do.  Having the application terminated because the `ssh` session to the edgenode timed out and your computer went to sleep will be very disruptive.

Using either of these session state tools (or another of your choice) while running it on an edgenode will allow you to sign-off without disrupting the process AND reattach to see the interactive progress at a later point.
</tip>

<tip>
Use `dryrun` FIRST

Before you run a process that will make changes, try running `hms-mirror` with the `dry-run` option first.  The report generated at the end of the job will provide insight into what issues (if any) you'll run across.

</tip>

<tip>
Start Small

Use `-db`(database) AND `-tf`(table filter) options to limit the scope of what you're processing.  Start with a test database that contains various table types you'd like to migrate.

Review the output report for details/problems you encountered in processing.

Since the process expects the user to have adequate permissions in both clusters, you may find you have some prework to do before continuing.
</tip>