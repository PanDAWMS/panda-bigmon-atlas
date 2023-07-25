#!/bin/bash
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh -3
export X509_USER_PROXY=$1
export RUCIO_ACCOUNT=$2
lsetup rucio
rucio upload --rse $3 $4 $5 --scope $6