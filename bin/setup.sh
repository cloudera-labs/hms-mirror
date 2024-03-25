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
