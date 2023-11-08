# Setup

## LEFT and RIGHT Clusters

`hms-mirror` defines clusters as LEFT and RIGHT.  The LEFT cluster is the source of the metadata and the RIGHT cluster is the target.  The LEFT cluster is usually the older cluster version.  Regardless, under specific scenario's, `hms-mirror` will use an HDFS client to check directories and move small amounts of data (AVRO schema files).  `hms-mirror` will depend on the configuration of the node it's running on to locate the 'hcfs filesystem'.  This means that the `/etc/hadoop/conf` directory should contain all the environments settings to successfully connect to `hcfs (Hadoop Compatible File System`.

## Binary Package

[Download the latest version of `hms-mirror-dist.tar.gz`](https://github.com/cloudera-labs/hms-mirror/releases)

## HMS-Mirror Setup from Binary Distribution

On the edgenode:
- Remove previous install directory `rm -rf hms-mirror-install`
  - If you don't remove the previous install directory, the default `tar` behaviour will NOT overwrite the existing directory, hence you won't get the new version.
- Expand the tarball `tar zxvf hms-mirror-dist.tar.gz`.
  > This produces a child `hms-mirror-install` directory.
- Two options for installation:
    - As the root user (or `sudo`), run `hms-mirror-install/setup.sh`. This will install the `hms-mirror` packages in `/usr/local/hms-mirror` and create symlinks for the executables in `/usr/local/bin`.  At this point, `hms-mirror` should be available to all user and in the default path.
    - As the local user, run `hms-mirror-install/setup.sh`.  This will install the `hms-mirror` packages in `$HOME/.hms-mirror` and create symlink in `$HOME/bin`.  Ensure `$HOME/bin` is in the users path and run `hms-mirror`.

*DO NOT RUN `hms-mirror` from the installation directory.*

If you install both options, your environment PATH will determine which one is run.  Make note of this because an upgrade may not be reachable.

## Quick Start

`hms-mirror` requires a configuration file describing the LEFT (source) and RIGHT (target) cluster connections.  There are two ways to create the config:

- `hms-mirror --setup` - Prompts a series of questions about the LEFT and RIGHT clusters to build the default configuration file.
- Use the [default config template](hms-mirror-Default-Configuration-Template.md)) as a starting point.  Edit and place a copy here `$HOME/.hms-mirror/cfg/default.yaml`.

If either or both clusters are Kerberized, please review the detailed configuration guidance [here](hms-mirror-running.md#running-against-a-legacy-non-cdp-kerberized-hiveserver2) and [here](hms-mirror-running.md#kerberized-connections).

## General Guidance

- Run `hms-mirror` from the RIGHT cluster on an Edge Node.
> `hms-mirror` is built (default setting) with CDP libraries and will connect natively using those libraries.  The edge node should have the hdfs client installed and configured for that cluster.  The application will use this connection for some migration strategies.
- If running from the LEFT cluster, note that the `-ro/--read-only` feature examines HDFS on the RIGHT cluster.  The HDFS client on the LEFT cluster may not support this access.
- Connecting to HS2 through KNOX (in both clusters, if possible) reduces the complexities of the connection by removing Kerberos from the picture.
- The libraries will only support a Kerberos connection to a 'single' version of Hadoop at a time.  This is relevant for 'Kerberized' connections to Hive Server 2.  The default libraries will support a kerberized connection to a CDP clusters HS2 and HDFS.  If the LEFT (source) cluster is Kerberized, including HS2, you will need to make some adjustments.
    - The LEFT clusters HS2 needs to support any auth mechanism BUT Kerberos.
    - Use an Ambari Group to setup an independent HS2 instance for this exercise or use KNOX.
