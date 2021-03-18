#!/usr/bin/env sh

# Should be run as root.

cd `dirname $0`

mkdir -p /usr/local/hms-mirror/bin
mkdir -p /usr/local/hms-mirror/lib

cp -f hms-mirror /usr/local/hms-mirror/bin

# Cleanup previous installation
rm -f /usr/local/hms-mirror/lib/*.jar

if [ -f ../target/hms-mirror-shaded.jar ]; then
    cp -f ../target/hms-mirror-shaded.jar /usr/local/hms-mirror/lib
fi
if [ -f ../target/hms-mirror-shaded-no-hadoop.jar ]; then
    cp -f ../target/hms-mirror-shaded-no-hadoop.jar /usr/local/hms-mirror/lib
fi

if [ -f hms-mirror-shaded.jar ]; then
    cp -f hms-mirror-shaded.jar /usr/local/hms-mirror/lib
fi
if [ -f hms-mirror-shaded-no-hadoop.jar ]; then
    cp -f hms-mirror-shaded-no-hadoop.jar /usr/local/hms-mirror/lib
fi

chmod -R +r /usr/local/hms-mirror
chmod +x /usr/local/hms-mirror/bin/hms-mirror

ln -sf /usr/local/hms-mirror/bin/hms-mirror /usr/local/bin/hms-mirror


