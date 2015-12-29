#!/bin/bash         
# 20151228 Scott Havens
#
# Download the metadata for all the sources

# download the Mesowest metadata
sh Mesowest/mesowestMetadata.sh

# add the variables for Mesowest
python Mesowest/updateMetadataVariables.py

# download the CDEC metadata
python CDEC/updateMetadata.py

# correct station locations if needed
python updateStationCoords.py