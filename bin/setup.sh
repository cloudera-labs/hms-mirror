#!/usr/bin/env sh
# Copyright 2021 Cloudera, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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

#if [ ! -f $HOME/.hms-mirror/cfg/default.yaml ]; then
#  cp default.template.yaml $HOME/.hms-mirror/cfg/default.yaml
#  echo "A default.yaml template has been copied to $HOME/.hms-mirror/cfg. Modify this for your environment."
#fi

# Cleanup previous installation
rm -f $BASE_DIR/lib/*.jar
rm -f $BASE_DIR/bin/*

cp -f hms-mirror $BASE_DIR/bin

if [ -f hms-mirror-shaded.jar ]; then
    cp -f hms-mirror-shaded.jar $BASE_DIR/lib
fi
if [ -f hms-mirror-shaded-no-hadoop.jar ]; then
    cp -f hms-mirror-shaded-no-hadoop.jar $BASE_DIR/lib
fi

chmod -R +r $BASE_DIR
chmod +x $BASE_DIR/bin/hms-mirror

if (( $EUID == 0 )); then
  echo "Setting up global links"
  ln -sf $BASE_DIR/bin/hms-mirror /usr/local/bin/hms-mirror
else
  mkdir -p $HOME/bin
  ln -sf $BASE_DIR/bin/hms-mirror $HOME/bin/hms-mirror
  echo "Executable in \$HOME/bin .  Add this to the environment path."
fi


