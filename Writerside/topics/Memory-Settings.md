# Memory Settings

The default memory footprint of `hms-mirror` is set in the startup script [here](https://github.com/cloudera-labs/hms-mirror/blob/560166ca90c0d7ce243f8215ba9f29ca1a10cba2/bin/hms-mirror#L70) 
which declares an 8GB max footprint.

If you need to increase this, you can opt to export `APP_JAVA_OPTS` before running `hms-mirror`.  You'll need to include:

- Xms
- Xmx
- Garbage Collection info.

For example:

`export APP_JAVA_OPTS="-Xms8192m -Xmx16384m -XX:+UseG1GC`

This raises the memory limit to 16Gb.