
CREATE PROCEDURE ORACLE_CDC(inCDCSchema VARCHAR(20), inCDCTable VARCHAR(40), inTargetSchema VARCHAR(20), inTargetTable VARCHAR(40), inFromTimeStamp VARCHAR(26), jdbcURL VARCHAR(100))
    LANGUAGE JAVA
    PARAMETER STYLE JAVA
    MODIFIES SQL DATA 
    EXTERNAL NAME 'com.splice.custom.trigger.ApplyCDC.UpdateTargetTable';
