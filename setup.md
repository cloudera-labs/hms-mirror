# Setup

## Obtaining HMS-Mirror

### Building

`mvn clean install`

Produces a 'tarball' that can be distributed.  Copy the `target/hms-mirror-<version>-dist.tar.gz` to an edge node on the **UPPER** cluster.

### Obtaining Pre-Built Binary

[![Download the LATEST Binary](./images/download.png)](https://github.com/dstreev/hms-mirror/releases)

## HMS-Mirror Setup from Binary Distribution

On the edgenode:
- Expand the tarball `tar zxvf hms-mirror-<version>-dist.tar.gz`.  
  > This produces a child `hms-mirror` directory.
- As the root user (or `sudo`), run `hms-mirror/setup.sh`.


