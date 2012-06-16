#!/bin/bash

export PYTHONPATH=$PYTHONPATH:/tmp/ezrcluster/src/python

mkdir /home/#USER#/.matplotlib
mkdir /home/#USER#/.matplotlib/tex.cache
chmod -R 777 /home/#USER#/.matplotlib

echo "Unpacking ezrcluster"
cd /tmp
tar xvzf ezrcluster.tgz
rm ezrcluster.tgz

echo "Sourcing application script..."
if [ -f /tmp/application-script.sh ]
then
    source /tmp/application-script.sh
fi

echo "PYTHONPATH=$PYTHONPATH"

#run ezrcluster Daemon
echo "Starting daemon..."
python /tmp/ezrcluster/src/python/ezrcluster/daemon.py --num_instances #NUM_INSTANCES# &
echo "Daemon started... all done!"