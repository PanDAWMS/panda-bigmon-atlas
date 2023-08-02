#!/bin/bash
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh -3
mkdir -p $1
cd $1 || exit
asetup $2
mkdir -p original
tar xf $3 -C original || exit
mkdir -p modified
cp -r original/* modified
cd modified || exit
python $4 $5 $6 ./jobdef.root || exit
tar cfz ../source.modified.tar ./*


