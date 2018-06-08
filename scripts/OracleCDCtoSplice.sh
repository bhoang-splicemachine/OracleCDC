#!/bin/bash

. /home/oracle/gg/S3scripts/config.properties
timestamp=`date --date='55 minutes ago' "+%Y-%m-%d %H:%M:%S"`

echo "Oracle CDC cron job starting at "${timestamp}
echo "*** TRANSFERRING ORACLE CDC FLAT FILES TO S3 ***"
${SCRIPTHOME}/upload_to_S3 || exit 1

echo "*** IMPORTING CDC FLAT FILES INTO SPLICE'S STAGING TABLE ***"
importedrows=false
{ output=$(${SCRIPTHOME}/splice_import_CDC_records 2>&1) ;} 2>&1
if [[ $output == *"rowsImported"* ]]
  then
	echo $output
	importedrows=true
  else
	echo "Did not import records\n"
	echo $output > ${SCRIPTHOME}/import.err
	cat ${SCRIPTHOME}/import.err
	exit 1
fi

if [ $importedrows == "true" ]
  then
      echo "*** ARCHIVING IMPORTED FILES ON S3 ***"
      for copiedfiles in `${AWSCLI} ${AWSCLI_LIST_OPTS} ${BUCKET}/ | awk '{ print $4}'` ; do
        echo "Moving ${BUCKET}/$copiedfiles to ${S3PROCESSEDDIR}/"
        ${AWSCLI} ${AWSCLI_MOVE_OPTS} ${BUCKET}/$copiedfiles ${S3PROCESSEDDIR}/  || exit 1
      done
  echo "*** APPLY CDC RECORDS TO SPLICE'S TARGET TABLE ***"
  ${SCRIPTHOME}/splice_apply_CDC_records ${timestamp}
fi
