#!/usr/bin/env sh
#
# Copyright (c) 2022. Cloudera, Inc. All Rights Reserved
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#

cd `dirname $0`

# Minimum required Java version
MIN_JAVA_VERSION=17

# Function to extract major version number from java -version output
get_java_version() {
    local version_output=$($1 -version 2>&1)
    # Extract version number, handling both 1.8.0_xxx and 17.0.1+ formats
    local version=$(echo "$version_output" | head -n 1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+|[0-9]+' | head -n 1)
    # For 1.x style, return the minor version (e.g., 8 for 1.8.0); for 17+ style, return major number
    if [[ "$version" =~ ^1\.[0-9]+\.[0-9]+$ ]]; then
        echo "$version" | cut -d'.' -f2
    else
        # Take only the major version (before the first dot)
        echo "$version" | cut -d'.' -f1
    fi
}

# Function to check if version meets minimum requirement
check_version() {
    local version=$1
    if [ -z "$version" ]; then
        return 1
    fi
    # Use numeric comparison to properly handle versions
    if [ "$version" -ge "$MIN_JAVA_VERSION" ]; then
        return 0
    else
        return 1
    fi
}

# Determine which Java to use
if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA_CMD="$JAVA_HOME/bin/java"
    echo "Using JAVA_HOME: $JAVA_HOME"
else
    JAVA_CMD="java"
    echo "JAVA_HOME not set, using default system Java"
fi

# Check if Java is available
if ! command -v "$JAVA_CMD" &> /dev/null; then
    echo "Error: Java not found on system!"
    echo "Please install Java $MIN_JAVA_VERSION or higher and set JAVA_HOME or ensure it's in PATH"
    exit 1
fi

# Get Java version for comparison
JAVA_VERSION=$(get_java_version "$JAVA_CMD")
# Get full version string for display
FULL_VERSION=$("$JAVA_CMD" -version 2>&1 | head -n 1 | grep -oE '"[^"]+"')

echo "Detected Java version: $FULL_VERSION (major version: $JAVA_VERSION)"

# Check if version meets requirements
if ! check_version "$JAVA_VERSION"; then
    echo "Error: Java version $FULL_VERSION is too old!"
    echo "Please upgrade to Java $MIN_JAVA_VERSION or higher"
    echo "If this Java Version isn't available, you should upgrade."
    echo "If that isn't possible, you'll need to use hms-mirror versions 2.3.x or earlier."
    exit 1
fi

if (( $EUID != 0 )); then
  echo "Setting up as non-root user"
  BASE_DIR=$HOME/.hms-mirror
else
  echo "Setting up as root user"
  BASE_DIR=/usr/local/hms-mirror
fi

mkdir -p $BASE_DIR/bin
mkdir -p $BASE_DIR/lib

mkdir -p $HOME/.hms-mirror/cfg
mkdir -p $HOME/.hms-mirror/aux_libs

# Cleanup previous installation
rm -f $BASE_DIR/lib/*.jar
rm -f $BASE_DIR/bin/*

cp -f hms-mirror $BASE_DIR/bin
cp -f hms-mirror-cli $BASE_DIR/bin

for jar in `ls lib/*.jar`; do
    cp -f $jar $BASE_DIR/lib
done

chmod -R +r $BASE_DIR
chmod +x $BASE_DIR/bin/hms-mirror
chmod +x $BASE_DIR/bin/hms-mirror-cli

if (( $EUID == 0 )); then
  echo "Setting up global links"
  ln -sf $BASE_DIR/bin/hms-mirror /usr/local/bin/hms-mirror
  ln -sf $BASE_DIR/bin/hms-mirror-cli /usr/local/bin/hms-mirror-cli
else
  mkdir -p $HOME/bin
  ln -sf $BASE_DIR/bin/hms-mirror $HOME/bin/hms-mirror
  ln -sf $BASE_DIR/bin/hms-mirror-cli $HOME/bin/hms-mirror-cli
  echo "Executable in $HOME/bin .  Add this to the environment path."
fi
