#!/bin/bash
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh -3
export X509_USER_PROXY=$1
lsetup panda
export PANDA_NICKNAME=$2
export PANDA_AUTH=x509_no_grid
prun --exec "echo upload_new_source > myout.txt"  --inTarBall $3 --outDS "$4" --nJobs 1 --output myout.txt