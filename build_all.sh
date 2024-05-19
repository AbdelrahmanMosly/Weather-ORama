#!/bin/bash

cd CentralStation
./build.sh
cd ../weatherstation
./build.sh

cd ../kafka
./build.sh

exit 0