# Notes

The application is built against CDP 7.1 binaries.

## Issue 1

Connecting to an HDP cluster running 2.6.5 with Binary protocol and Kerberos triggers an incompatibility issue:
`Unrecognized Hadoop major version number: 3.1.1.7.1....`

### Solution

The application is built with CDP libraries (excluding the Hive Standalone Driver).  When `kerberos` is the `auth` protocol to connect to **Hive 1**, it will get the application libs which will NOT be compatible with the older cluster.

Kerberos connections are only supported to the CDP cluster.  

When connecting via `kerberos`, you will need to include the `--hadoop-classpath` when launching the application.     

## Features

- Add PARTITION handling
- Add Stages
- Add Check for existing table on upper cluster.
    - option for override def in stage 1.