#!/bin/bash         
# 20141129 Scott Havens
#
# Download current Mesowest data and store into a current and station database.
# Use: ./weather.sh > MesowestLog.txt 2>&1

printf '%50s\n' | tr ' ' -

# top level directory for the WeatherDB
path=/Users/scott/Documents/Projects/Snowserver/DownloadData

# MySQL login info
source "$path/db_connect.sh"

# location to save the data file
dloc="$path/Mesowest/data/mesowest_csv.tbl.gz"

# Download the data file
echo -e "Downloading mesowest_csv.tbl from Mesowest...";
wget --no-verbose -O $dloc http://mesowest.utah.edu/data/mesowest_csv.tbl.gz

if [ $? -ne 0 ]; then
	echo "wget exited with code $?";
	rm -f $dloc
	exit $?;
fi

echo -e "\n";

# unzip the data file, overwrite mesowest.dat
gunzip -f $dloc
rm -f $dloc

# Have to change all empty fields to \N
echo -e "Changing empty fields to \N...";
/usr/bin/time awk -F, '{for(i=1;i<=NF;++i){if($i==""){printf "\\N"}else{printf $i} if(i<NF)printf ","} printf "\n"}' "$path/Mesowest/data/mesowest_csv.tbl" > "$path/Mesowest/data/mesowest_csv.awk"
rm -f $path/Mesowest/data/mesowest_csv.tbl
echo -e "\n";

# add a source value to the output
awk 'BEGIN{FS = OFS = ","}{$(NF+1) = NR==1 ? "source" : "Mesowest"}1' "$path/Mesowest/data/mesowest_csv.awk" > "$path/Mesowest/data/mesowest_metadata.awk"
rm -f $path/Mesowest/data/mesowest_csv.awk


# load mesowest.dat into the current table
echo -e "Loading data into weather_v2.tbl_metadata...";

/usr/bin/time mysql --local-infile --show-warnings --host="$host" --user="$user" --password="$pass" $db < "$path/Mesowest/query/LoadMetadata.sql"

#rm -f $path/data/mesowest_csv.awk

if [ $? -ne 0 ]; then
	echo "MySQL LoadMetadata exited with code $?";
	exit $?;
fi

# Change the metadata table for SAC
#echo -e "Changing metadata for SAC";

#/usr/bin/time -f "%e seconds" mysql --show-warnings --host="$host" --user="$user" --password="$pass" $db < "$path/query/SNFAC_Metadata.sql"





