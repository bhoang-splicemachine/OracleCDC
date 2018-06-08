package com.splice.custom.trigger;

/**
 * Created by bhoang on 6/1/17.
 */

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Logger;

public class ApplyCDC {

    public static void UpdateTargetTable (final String inCDCSchema, final String inCDCTable, final String inTargetSchema,
                                     final String inTargetTable, final String inFromTimeStamp, final String inJDBCURL) {

        String [] targetTableColumnNames = null;
        String [] PKColumnNames = null;
        Integer rsUPSERTResult;
        Integer rsDeleteResult;
        Integer rsVerifiedCDCRecords;
        //MyFormatter MyLogger = null;

        //MyLogger.initLogger("test", "001");
        //MyLogger.LOGGER.info("Testing message");

        try (Connection conn = getConnection(inJDBCURL)) {
            //Create a connection

            String tempCDCTable = "tempCDC" + new SimpleDateFormat("yyyyMMddHHmm").format(new Date().getTime());
            PKColumnNames = ObtainPKColumns(conn, inTargetSchema, inTargetTable);
            LoadEffectiveChangedRecords (conn, inCDCSchema, inCDCTable, tempCDCTable, PKColumnNames, inFromTimeStamp);
            targetTableColumnNames = ConstructColumnList(conn, inTargetSchema, inTargetTable);
            //System.out.print(targetTableColumnNames.length + "\n");
                if (targetTableColumnNames.length >= 1) {
                    /*for (int i = 1; i <= targetTableColumnNames.length; i++) {
                        LOGGER.info(targetTableColumnNames[i-1]);
                    }*/
                } else {
                    LOGGER.info("Table " + inTargetTable + "has zero columns \n");
                }
            String stmtUPSERTSQL = ConstructUPSERTSQL(inCDCSchema, tempCDCTable, inTargetSchema, inTargetTable, inFromTimeStamp, targetTableColumnNames);
            //System.out.print(stmtUPSERTSQL + " \n");
            // Perform UPSERT for records with Opcode in 'I' or 'U'
            assert conn != null;
            try (PreparedStatement ps = conn.prepareStatement(stmtUPSERTSQL)) {
                rsUPSERTResult = ps.executeUpdate();
                //System.out.print("UPSERT Results : " + rsUPSERTResult + " \n");
            } catch (SQLException e) {
                conn.rollback();
                conn.close();
                e.printStackTrace();
                throw e;
            }
            String stmtDeleteSQL = ConstructDeleteSQL(inCDCSchema, tempCDCTable, inTargetSchema, inTargetTable, inFromTimeStamp, PKColumnNames);
            //System.out.print(stmtDeleteSQL + " \n");
            // Perform DELETE on records with Opcode = 'D'
            try (PreparedStatement ps = conn.prepareStatement(stmtDeleteSQL)) {
                rsDeleteResult = ps.executeUpdate();
                //System.out.print("DELETE Results : " + rsDeleteResult + " \n");
            } catch (SQLException e) {
                conn.rollback();
                conn.close();
                e.printStackTrace();
                throw e;
            }

            rsVerifiedCDCRecords = VerifyandUpdateRawCDCRecords(conn, inCDCSchema, inCDCTable, tempCDCTable, PKColumnNames, inFromTimeStamp,
                    rsUPSERTResult, rsDeleteResult);
            // if successfully verified processed CDC records then drop temp CDC table
            if (rsVerifiedCDCRecords > 0) {
                DropTempCDCTable (conn, inCDCSchema, tempCDCTable);
            }
            conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }
    private static Timestamp convertTimeStamp (final String inDateTime) {

        final DateTimeParser[] parsers = {
                DateTimeFormat.forPattern( "yyyy-MM-dd HH:mm:ss.SSS" ).getParser(),
                DateTimeFormat.forPattern( "yyyy-MM-dd HH:mm:ss.SS" ).getParser(),
                DateTimeFormat.forPattern( "yyyy-MM-dd HH:mm:ss.S" ).getParser(),
                DateTimeFormat.forPattern( "yyyy-MM-dd HH:mm:ss" ).getParser(),
                DateTimeFormat.forPattern( "yyyy-MM-dd'T'HH:mm:ss.SSS" ).getParser(),
                DateTimeFormat.forPattern( "yyyy-MM-dd'T'HH:mm:ss.SS" ).getParser(),
                DateTimeFormat.forPattern( "yyyy-MM-dd'T'HH:mm:ss.S" ).getParser(),
                DateTimeFormat.forPattern( "yyyy-MM-dd'T'HH:mm:ss" ).getParser() };

        DateTimeFormatter formatter = new DateTimeFormatterBuilder().append( null, parsers ).toFormatter();
        final DateTime dt = formatter.parseDateTime(inDateTime);
        final String timezoneID = TimeZone.getDefault().getID();
        final DateTime dateTime = new DateTime(dt, DateTimeZone.forID(timezoneID));

        return new Timestamp(dateTime.getMillis());
    }

    private static Connection getConnection(final String jdbcURL) throws SQLException {
        try {
            Class.forName("com.splicemachine.db.jdbc.ClientDriver");
        } catch (ClassNotFoundException cne){
            cne.printStackTrace();
            return null;
        }

        return DriverManager.getConnection(jdbcURL);
        //return DriverManager.getConnection("jdbc:default:connection");
        //return DriverManager.getConnection("jdbc:splice://stl-colo-srv065:1527/splicedb;user=splice;password=admin");
    }

    private static String[] ConstructColumnList (final Connection inConn, final String inSchema, final String inTable) throws SQLException {
        String selectTableMeta = "SELECT *\n" +
                "FROM " + inSchema + "." + inTable + "\n" +
                "WHERE 1=0";
        String[] columnNames = null;
        try (PreparedStatement ps = inConn.prepareStatement(selectTableMeta)) {
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            //System.out.print(columnCount + "\n");
            columnNames = new String[columnCount];

            // The column count starts from 1
            if (columnCount >= 1) {
                for (int i = 1; i <= columnCount; i++) {
                    columnNames[i-1] = rsmd.getColumnName(i);
                    //System.out.print(columnNames[i-1]);
                }
            } else {
                LOGGER.info("Table " + inTable + "has zero columns \n");
            }
        } catch (SQLException e) {
            inConn.rollback();
            inConn.close();
            e.printStackTrace();
            throw e;
        }
        return columnNames;
    }

    private static String ConstructUPSERTSQL (final String tempCDCSchema, final String tempCDCTable, final String inSchema,
                                             final String inTable, final String inTransTimeStamp, final String [] inColumnList) {
        String UPSERTSQL = null;
        //String columnListStr = null;
        StringBuilder columnListSB = new StringBuilder();
        String columnListStr = null;

        if (inColumnList.length >= 1) {
            for (int i = 1; i <= inColumnList.length; i++) {
                columnListSB.append(inColumnList[i - 1]);
                columnListSB.append(',');
            }
            columnListStr = StringUtils.removeEnd(columnListSB.toString(),",");
        } else {
            LOGGER.info("Column list for table " + inSchema + "." + inTable + "is empty \n");
        }

        String tempSQL = "INSERT INTO " + inSchema + "." + inTable + "( \n" +
                            columnListStr + ")";
        String optionsSQL = " --splice-properties insertMode=UPSERT \n";
        UPSERTSQL = tempSQL + optionsSQL + "SELECT " + columnListStr + " FROM " + tempCDCSchema + "." + tempCDCTable +
                    " WHERE opcode IN ('I','U') and transtimestamp >= '" + inTransTimeStamp + "' and processed = 'N'";
        return UPSERTSQL;
    }

    private static String ConstructDeleteSQL (final String tempCDCSchema, final String tempCDCTable, final String inSchema,
                                              final String inTable, final String inTransTimeStamp, final String [] inColumnList) {
        String DeleteSQL = null;
        //String columnListStr = null;
        StringBuilder columnListSB = new StringBuilder();
        String columnListStr = null;

        if (inColumnList.length >= 1) {
            for (int i = 1; i <= inColumnList.length; i++) {
                if (inColumnList[i - 1] != null) {
                    columnListSB.append("ST.").append(inColumnList[i - 1]).append(" = T.").append(inColumnList[i - 1]);
                    columnListSB.append(" and ");
                }
            }
            columnListStr = StringUtils.removeEnd(columnListSB.toString()," and ");
        } else {
            LOGGER.info("Specified PK column list for table " + inSchema + "." + inTable + "is empty \n");
        }

        DeleteSQL = "DELETE FROM " + inSchema + "." + inTable + " T WHERE EXISTS ( \n" +
                "SELECT * FROM " + tempCDCSchema + "." + tempCDCTable + " ST WHERE (" + columnListStr + ") \n" +
                "and ST.OPCODE = 'D' and transtimestamp >= '" + inTransTimeStamp + "' and ST.PROCESSED = 'N')";
        return DeleteSQL;
    }


    private static String[] ObtainPKColumns (final Connection inConn, final String inSchema, final String inTable) throws SQLException {
        String selectTablePK = "CALL SYSIBM.SQLPRIMARYKEYS(null,\n" +
                "'" + inSchema + "','" + inTable + "',null)";
        //System.out.print(selectTablePK + " \n");
        String[] tempPKColumnNames = null;
        String[] finalPKColumnNames = null;
        StringBuilder PKColumnList = new StringBuilder();
        int pkCols = 10;

        try (PreparedStatement ps = inConn.prepareStatement(selectTablePK)) {
            ResultSet rs = ps.executeQuery();
            tempPKColumnNames = new String[pkCols];
            int i = 1;
            if(rs.next()) {
                do { // Use 'do...while' to process the first row, while continuing to process remaining rows
                        tempPKColumnNames[i - 1] = rs.getString("COLUMN_NAME");
                        i++;
                } while (rs.next());
            }

            // The column count starts from 1
            if (tempPKColumnNames.length >= 1) {
                finalPKColumnNames = new String[tempPKColumnNames.length];
                for (int j = 1; j <= i; j++) {
                    if (tempPKColumnNames[j-1] != null) {
                        finalPKColumnNames[j - 1] = tempPKColumnNames[j - 1];
                        PKColumnList.append(tempPKColumnNames[j - 1]).append(',');
                    }
                }
                LOGGER.info("PRIMARY KEY columns for table " + inTable + ":" + StringUtils.removeEnd(PKColumnList.toString(),","));
            } else {
                LOGGER.info("Table " + inTable + "has no PK columns \n");
            }
        } catch (SQLException e) {
            inConn.rollback();
            inConn.close();
            e.printStackTrace();
            throw e;
        }
        return finalPKColumnNames;
    }

    private static void LoadEffectiveChangedRecords (final Connection inConn, final String CDCSchema, final String sourceCDCTable,
                                                     final String tempCDCTable, final String [] inPKColumnList,
                                                     final String inTransTimeStamp) throws SQLException {
        String createTempCDCTable = "create table " +
                CDCSchema + "." + tempCDCTable + " as select * from \n" +
                CDCSchema + "." + sourceCDCTable + " with no data";

        StringBuilder whereColumnListSB = new StringBuilder();
        StringBuilder columnList = new StringBuilder();
        String whereColumnListSBstr = null;
        String columnListstr = null;
        Integer rsCreateTempCDCResult;
        Integer rsInsertTempCDCResult;
        ResultSet rsCountQualifiedRecordsResult;
        Integer countQualifiedRecords;

        // Construct WHERE clause between source and temp CDC tables using PKs, and also flatten column list from array
        if (inPKColumnList.length >= 1) {
            for (int i = 1; i <= inPKColumnList.length; i++) {
                if (inPKColumnList[i - 1] != null) {
                    whereColumnListSB.append("SOURCE.").append(inPKColumnList[i - 1]).append(" = LATESTCHANGE.").append(inPKColumnList[i - 1]);
                    whereColumnListSB.append(" and ");
                    columnList.append(inPKColumnList[i - 1]).append(",");
                }
            }
            //Remove the last "and" and "," from column list
            whereColumnListSBstr = StringUtils.removeEnd(whereColumnListSB.toString(), " and ");
            columnListstr = StringUtils.removeEnd(columnList.toString(), ",");
        } else {
            LOGGER.info("Specified PK column list is empty \n");
        }

        String selectCountofQualifiedRawRecords = "select count(*) rowcount from " + CDCSchema + "." + sourceCDCTable +
                " where PROCESSED = 'N' and TRANSTIMESTAMP >= '" + inTransTimeStamp + "'";

        String selectEffectiveRecords = " select SOURCE.* from " + CDCSchema + "." + sourceCDCTable + " SOURCE join " +
                "(select max(TRANSTIMESTAMP) maxdate," + columnListstr +
                " from " + CDCSchema + "." + sourceCDCTable + " B where PROCESSED = 'N' and " +
                "TRANSTIMESTAMP >= '" + inTransTimeStamp + "' group by " + columnListstr + ") \n" +
                " as LATESTCHANGE on (" + whereColumnListSBstr + " and SOURCE.TRANSTIMESTAMP = " +
                "maxdate and PROCESSED = 'N' and TRANSTIMESTAMP >= '" + inTransTimeStamp + "')" +
                " order by SOURCE.TRANSTIMESTAMP";
        String insertEffectiveRecords = "insert into " + CDCSchema + "." + tempCDCTable + "\n" +
                "--splice-properties insertMode=INSERT, skipConflictDetection=true, skipWAL=true \n" +
                selectEffectiveRecords;

        try (PreparedStatement ps = inConn.prepareStatement(selectCountofQualifiedRawRecords)) {
            rsCountQualifiedRecordsResult = ps.executeQuery();
            rsCountQualifiedRecordsResult.next();
            countQualifiedRecords = rsCountQualifiedRecordsResult.getInt("rowcount");
        } catch (SQLException e) {
            inConn.rollback();
            inConn.close();
            e.printStackTrace();
            throw e;
        }

        if (countQualifiedRecords > 0) {  //if there are qualified rows from staging tables
            // Create temp CDC Table
            try (PreparedStatement ps = inConn.prepareStatement(createTempCDCTable)) {
                rsCreateTempCDCResult = ps.executeUpdate();
            } catch (SQLException e) {
                inConn.rollback();
                inConn.close();
                e.printStackTrace();
                throw e;
            }

            // Insert effective changed records into temp CDC Table
            try (PreparedStatement ps = inConn.prepareStatement(insertEffectiveRecords)) {
                rsInsertTempCDCResult = ps.executeUpdate();
                LOGGER.info("Number of qualified records in CDC staging table: " + countQualifiedRecords);
                LOGGER.info("Number of effective records to be processed: " + rsInsertTempCDCResult);
            } catch (SQLException e) {
                inConn.rollback();
                inConn.close();
                e.printStackTrace();
                throw e;
            }
        } else {
            LOGGER.info("There are no qualified CDC records to be processed...exiting");
            System.exit(0);
        }
    }

    private static int VerifyandUpdateRawCDCRecords (final Connection inConn, final String CDCSchema, final String sourceCDCTable,
                                                     final String tempCDCTable, final String [] inPKColumnList,
                                                     final String inTransTimeStamp, final Integer UPSERTedrowcount,
                                                     final Integer Deletedrowcount) throws SQLException {

        int rsUpdatedCDCResult;
        StringBuilder columnListSB = new StringBuilder();
        String columnListStr = null;

        if (inPKColumnList.length >= 1) {
            for (int i = 1; i <= inPKColumnList.length; i++) {
                if (inPKColumnList[i - 1] != null) {
                    columnListSB.append("B.").append(inPKColumnList[i - 1]).append(" = A.").append(inPKColumnList[i - 1]);
                    columnListSB.append(" and ");
                }
            }
            columnListStr = StringUtils.removeEnd(columnListSB.toString()," and ");
        } else {
            LOGGER.info("Specified PK column list is empty \n");
        }

        // Construct SQL to update raw CDC table with processed='Y' for records that were updated on target table
        String updateRawCDCRecords = "update " + CDCSchema + "." + sourceCDCTable + " A set PROCESSED='Y' " +
                        "where exists (select position from " + CDCSchema + "." + tempCDCTable + " B where " +
                        columnListStr + " and PROCESSED = 'N' and TRANSTIMESTAMP >= '" + inTransTimeStamp + "')";

        //System.out.print(updateRawCDCRecords + "\n");
        // Create temp CDC Table
        LOGGER.info("Number of effective records UPSERTED in target table: " + UPSERTedrowcount + "\n");
        LOGGER.info("Number of effective records DELETED from target table: " + Deletedrowcount + "\n");

        try (PreparedStatement ps = inConn.prepareStatement(updateRawCDCRecords)) {
            rsUpdatedCDCResult = ps.executeUpdate();
            LOGGER.info("Successfully updated status of " + rsUpdatedCDCResult + " raw CDC records \n");
        } catch (SQLException e) {
            inConn.rollback();
            inConn.close();
            LOGGER.info("Error: Failed to update status of raw CDC records \n");
            e.printStackTrace();
            throw e;
        }
        return rsUpdatedCDCResult;
    }


    private static int DropTempCDCTable (final Connection inConn, final String CDCSchema,
                                                     final String tempCDCTable) throws SQLException {

        int rsDropCDCResult;

        try (PreparedStatement ps = inConn.prepareStatement("drop table " + CDCSchema + "." + tempCDCTable)) {
            rsDropCDCResult = ps.executeUpdate();
            LOGGER.info("Successfully dropped temporary CDC table: " + CDCSchema + "." + tempCDCTable + "\n");
        } catch (SQLException e) {
            inConn.rollback();
            inConn.close();
            LOGGER.info("Error: Failed to drop temporary CDC table: " + CDCSchema + "." + tempCDCTable + "\n ");
            e.printStackTrace();
            throw e;
        }
        return rsDropCDCResult;
    }

    private final static Logger LOGGER = Logger.getLogger("ApplyCDC");

    /*public class MyFormatter extends Formatter {

        @Override
        public String format(LogRecord record) {
            return record.getLevel() + ":" + record.getMessage();
        }
    }*/

    //The following main method was used for unit testing of this UDF
    /*public static void main (String [] args)
    {
        UpdateTargetTable("APL","SHIPMENT_STAGING_TABLE","APL","SHIPMENT", "2017-10-07 02:00:29", "jdbc:splice://stl-colo-srv065:1527/splicedb;user=splice;password=admin");
    }*/
}
