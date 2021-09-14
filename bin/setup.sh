#!/usr/bin/env sh

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


