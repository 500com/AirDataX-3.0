package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.core.util.container.JarLoader;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public final class DBUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);

    private static final ThreadLocal<ExecutorService> rsExecutors = new ThreadLocal<ExecutorService>() {
        @Override
        protected ExecutorService initialValue() {
            return Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
                    .setNameFormat("rsExecutors-%d")
                    .setDaemon(true)
                    .build());
        }
    };

    private DBUtil() {
    }

    public static String chooseJdbcUrl(final DataBaseType dataBaseType,
                                       final List<String> jdbcUrls, final String username,
                                       final String password, final List<String> preSql,
                                       final boolean checkSlave) {

        if (null == jdbcUrls || jdbcUrls.isEmpty()) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONF_ERROR,
                    String.format("您的jdbcUrl的配置信息有错, 因为jdbcUrl[%s]不能为空. 请检查您的配置并作出修改.",
                            StringUtils.join(jdbcUrls, ",")));
        }

        try {
            return RetryUtil.executeWithRetry(new Callable<String>() {

                @Override
                public String call() throws Exception {
                    boolean connOK = false;
                    for (String url : jdbcUrls) {
                        if (StringUtils.isNotBlank(url)) {
                            url = url.trim();
                            if (null != preSql && !preSql.isEmpty()) {
                                connOK = testConnWithoutRetry(dataBaseType,
                                        url, username, password, preSql);
                            } else {
                                connOK = testConnWithoutRetry(dataBaseType,
                                        url, username, password, checkSlave);
                            }
                            if (connOK) {
                                return url;
                            }
                        }
                    }
                    throw new Exception("DataX无法连接对应的数据库，可能原因是：1) 配置的ip/port/database/jdbc错误，无法连接。2) 配置的username/password错误，鉴权失败。请和DBA确认该数据库的连接信息是否正确。");
//                    throw new Exception(DBUtilErrorCode.JDBC_NULL.toString());
                }
            }, 7, 1000L, true);
            //warn: 7 means 2 minutes
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONN_DB_ERROR,
                    String.format("数据库连接失败. 因为根据您配置的连接信息,无法从:%s 中找到可连接的jdbcUrl. 请检查您的配置并作出修改.",
                            StringUtils.join(jdbcUrls, ",")), e);
        }
    }

    public static String chooseJdbcUrlWithoutRetry(final DataBaseType dataBaseType,
                                       final List<String> jdbcUrls, final String username,
                                       final String password, final List<String> preSql,
                                       final boolean checkSlave) throws DataXException {

        if (null == jdbcUrls || jdbcUrls.isEmpty()) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONF_ERROR,
                    String.format("您的jdbcUrl的配置信息有错, 因为jdbcUrl[%s]不能为空. 请检查您的配置并作出修改.",
                            StringUtils.join(jdbcUrls, ",")));
        }

        boolean connOK = false;
        for (String url : jdbcUrls) {
            if (StringUtils.isNotBlank(url)) {
                url = url.trim();
                if (null != preSql && !preSql.isEmpty()) {
                    connOK = testConnWithoutRetry(dataBaseType,
                            url, username, password, preSql);
                } else {
                    try {
                        connOK = testConnWithoutRetry(dataBaseType,
                                url, username, password, checkSlave);
                    } catch (Exception e) {
                        throw DataXException.asDataXException(
                                DBUtilErrorCode.CONN_DB_ERROR,
                                String.format("数据库连接失败. 因为根据您配置的连接信息,无法从:%s 中找到可连接的jdbcUrl. 请检查您的配置并作出修改.",
                                        StringUtils.join(jdbcUrls, ",")), e);
                    }
                }
                if (connOK) {
                    return url;
                }
            }
        }
        throw DataXException.asDataXException(
                DBUtilErrorCode.CONN_DB_ERROR,
                String.format("数据库连接失败. 因为根据您配置的连接信息,无法从:%s 中找到可连接的jdbcUrl. 请检查您的配置并作出修改.",
                        StringUtils.join(jdbcUrls, ",")));
    }

    /**
     * 检查slave的库中的数据是否已到凌晨00:00
     * 如果slave同步的数据还未到00:00返回false
     * 否则范围true
     *
     * @author ZiChi
     * @version 1.0 2014-12-01
     */
    private static boolean isSlaveBehind(Connection conn) {
        try {
            ResultSet rs = query(conn, "SHOW VARIABLES LIKE 'read_only'");
            if (DBUtil.asyncResultSetNext(rs)) {
                String readOnly = rs.getString("Value");
                if ("ON".equalsIgnoreCase(readOnly)) { //备库
                    ResultSet rs1 = query(conn, "SHOW SLAVE STATUS");
                    if (DBUtil.asyncResultSetNext(rs1)) {
                        String ioRunning = rs1.getString("Slave_IO_Running");
                        String sqlRunning = rs1.getString("Slave_SQL_Running");
                        long secondsBehindMaster = rs1.getLong("Seconds_Behind_Master");
                        if ("Yes".equalsIgnoreCase(ioRunning) && "Yes".equalsIgnoreCase(sqlRunning)) {
                            ResultSet rs2 = query(conn, "SELECT TIMESTAMPDIFF(SECOND, CURDATE(), NOW())");
                            DBUtil.asyncResultSetNext(rs2);
                            long secondsOfDay = rs2.getLong(1);
                            return secondsBehindMaster > secondsOfDay;
                        } else {
                            return true;
                        }
                    } else {
                        LOG.warn("SHOW SLAVE STATUS has no result");
                    }
                }
            } else {
                LOG.warn("SHOW VARIABLES like 'read_only' has no result");
            }
        } catch (Exception e) {
            LOG.warn("checkSlave failed, errorMessage:[{}].", e.getMessage());
        }
        return false;
    }

    /**
     * 检查表是否具有insert 权限
     * insert on *.* 或者 insert on database.* 时验证通过
     * 当insert on database.tableName时，确保tableList中的所有table有insert 权限，验证通过
     * 其它验证都不通过
     *
     * @author ZiChi
     * @version 1.0 2015-01-28
     */
    public static boolean hasInsertPrivilege(DataBaseType dataBaseType, String jdbcURL, String userName, String password, List<String> tableList) {
        /*准备参数*/

        String[] urls = jdbcURL.split("/");
        String dbName;
        if (urls != null && urls.length != 0) {
            dbName = urls[3];
        }else{
            return false;
        }

        String dbPattern = "`" + dbName + "`.*";
        Collection<String> tableNames = new HashSet<String>(tableList.size());
        tableNames.addAll(tableList);

        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        try {
            ResultSet rs = query(connection, "SHOW GRANTS FOR " + userName);
            while (DBUtil.asyncResultSetNext(rs)) {
                String grantRecord = rs.getString("Grants for " + userName + "@%");
                String[] params = grantRecord.split("\\`");
                if (params != null && params.length >= 3) {
                    String tableName = params[3];
                    if (params[0].contains("INSERT") && !tableName.equals("*") && tableNames.contains(tableName))
                        tableNames.remove(tableName);
                } else {
                    if (grantRecord.contains("INSERT") ||grantRecord.contains("ALL PRIVILEGES")) {
                        if (grantRecord.contains("*.*"))
                            return true;
                        else if (grantRecord.contains(dbPattern)) {
                            return true;
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Check the database has the Insert Privilege failed, errorMessage:[{}]", e.getMessage());
        }
        if (tableNames.isEmpty())
            return true;
        return false;
    }

    public static boolean checkTableExist(DataBaseType dataBaseType, String jdbcURL, String userName, String password,String tableName){
        boolean flag = false;
        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        ResultSet rs = null;
        try {
            DatabaseMetaData meta = connection.getMetaData();
            String type [] = {"TABLE"};
            rs = meta.getTables(null, null, tableName, type);
            flag = rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.warn("cant connect to {},{}",jdbcURL,userName);
        }finally {
            closeDBResources(rs,null,connection);
        }
        return flag;
    }

    public static boolean checkTableExist(Connection connection,String tableName) {
        boolean flag = false;
        ResultSet rs = null;
        try{
            DatabaseMetaData meta = connection.getMetaData();
            String type [] = {"TABLE","VIEW"};
//            System.out.println("List of tables: ");
//            while (res.next()) {
//                System.out.println(
//                        "   "+res.getString("TABLE_CAT")
//                                + ", "+res.getString("TABLE_SCHEM")
//                                + ", "+res.getString("TABLE_NAME")
//                                + ", "+res.getString("TABLE_TYPE")
//                                + ", "+res.getString("REMARKS"));
//            }
            //String type [] = null;
            rs = meta.getTables(null, null, tableName, type);
            flag = rs.next();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        //return  false;
        return flag;
    }


    public static boolean checkTableExist(Connection connection,String tableName,DataBaseType dbtype) {
        boolean flag = false;
        ResultSet rs = null;
        ResultSet rsPattern = null;
        String tableNamePattern = tableName;
        try{
            DatabaseMetaData meta = connection.getMetaData();
            String type [] = {"TABLE","VIEW"};

            if(dbtype == DataBaseType.Oracle) {
                tableNamePattern = tableNamePattern.toUpperCase();
            }else if(dbtype == DataBaseType.PostgreSQL) {
                tableNamePattern = tableNamePattern.toLowerCase();
            }

            rs = meta.getTables(null, null, tableName, type);
            rsPattern = meta.getTables(null, null, tableNamePattern, type);
            flag = rs.next() || rsPattern.next();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        //return  false;
        return flag;
    }

    /**
     * 添加hive分区，修正path和列columns
     * @param dest
     * @param conn
     * @param table
     */
    public static void dealHive(Configuration dest,Connection conn,String table) {
        String path = dest.getString("tablePath");
        ArrayList<HashMap<String,String>> columnConfig = new ArrayList<HashMap<String,String>>();
        Statement destStmt = null;
        try {
            destStmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
            destStmt.setFetchSize(1);
            destStmt.execute("set hive.resultset.use.unique.column.names=false");
            destStmt.execute("set hive.execution.engine=mr");

            ResultSet rs = destStmt.executeQuery(String.format("select * from %s where 1=2",table));
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            for(int i=1;i<=columnCount;i++) {
                String type = null;
                String typeInfo = rsmd.getColumnTypeName(i);
                String colName = rsmd.getColumnName(i);
                int scale = rsmd.getScale(i);
                int index = typeInfo.indexOf("(");
                if(index<0) {
                    type = typeInfo;
                }else {
                    type = typeInfo.substring(0,typeInfo.indexOf("("));
                }
                if("decimal".equalsIgnoreCase(type) || "numeric".equalsIgnoreCase(type)) {
                    if(scale==0) {
                        type = "BIGINT";
                    }else {
                        type = "DOUBLE";
                    }
                }

                if("binary".equalsIgnoreCase(type) || "varbinary".equalsIgnoreCase(type)) {
                    type = "String";
                }

                HashMap<String,String> col = new HashMap<String, String>();
                col.put("name", colName);
                col.put("type", type);
                columnConfig.add(col);
            }

            dest.set("column", columnConfig);


            boolean isHivePartitionTable = dest.getBool("isHivePartitioned",false);

            if(isHivePartitionTable) {
                List<String> valueList = dest.getList("partitionValueList",String.class);
                int partitionColumnCount = dest.getInt("partitionColumnCount");
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("alter table %s add partition(",table));

                for(int i=columnCount-partitionColumnCount+1;i<=columnCount;i++) {
                    String colName = rsmd.getColumnLabel(i);
                    sb.append(colName + "=\"" + valueList.get(i+partitionColumnCount-columnCount-1) + "\"");
                    if(i!=columnCount)
                        sb.append(",");
                    path = path+ "/"+ colName+ "="+valueList.get(i+partitionColumnCount-columnCount-1);
                }
                sb.append(")");
                sb.append(String.format(" location '%s'",path));
                LOG.info("set hdfswriter path",path);
                dest.set("path",path);
                LOG.info("add partition:{}",sb.toString());
                destStmt.executeUpdate(sb.toString());
                destStmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * 删除指定表
     * @param destConn
     * @param type
     * @param tableName
     */
    public static void dropTable(Connection destConn,DataBaseType type,String tableName) {
        String dropTable = "drop table "+tableName;
        Statement st = null;
        try {
             st = destConn.createStatement();
             st.executeUpdate(dropTable);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(st!=null)
                try {
                    st.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
    }

    /**
     * 必须保证src中有标的表，而dest中无
     * @param srcConn
     * @param srcType
     * @param destConn
     * @param destType
     * @param table
     */
    public static void createTableWithConfig(Connection srcConn , DataBaseType srcType,String srcTable,Connection destConn,DataBaseType destType,String table,String columns,Configuration dest) {
        String querySql = buildQuerySql(srcTable,columns);
        createTableWithConfig(srcConn, srcType, querySql, destConn, destType, table,dest);
    }


    private static String buildQuerySql(String table,String columns) {
        String blank = " ";
        StringBuilder sb = new StringBuilder();
        sb.append("select");
        sb.append(blank);
        sb.append(columns);
        sb.append(blank);
        sb.append("from");
        sb.append(blank);
        sb.append(table);
        sb.append(blank);
        sb.append("where 1=2");

        return sb.toString();
    }

    /**
     * 必须保证src中有标的表，而dest中无
     * hive 需要更新dest这方的plugin的配置信息，去匹配hdfswriter中的path和column信息
     * @param srcConn
     * @param srcType
     * @param destConn
     * @param destType
     * @param table 需要建表的名称
     */
    public static void createTableWithConfig(Connection srcConn , DataBaseType srcType,String querySql,Connection destConn,DataBaseType destType,String table,Configuration dest) {

        if(!querySql.endsWith("where 1=2")) //无限制条件
            querySql = String.format(" select * from ( %s ) datax_local_subquery where 1=2 ",querySql);//添加限制条件。这里query当作子查询并加条件限制。依赖数据库的解析器是否能优化执行。

        LOG.warn("create table for table {}",table);
        LOG.warn("create table as query [{}]",querySql);

        Statement stmt = null;
        Statement destStmt = null;
        try {
            srcConn.setAutoCommit(false);
            stmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(1);

            if(srcType == DataBaseType.Hive) {
                //hive returns column name with table name,set this to false to disable it
                stmt.execute("set hive.resultset.use.unique.column.names=false");
                stmt.execute("set hive.execution.engine=mr");
            }

            boolean isHivePartitioned = false;
            if(destType == DataBaseType.Hive && dest.getBool("isHivePartitioned")) {
                isHivePartitioned = true;
            }

            ResultSet rs = stmt.executeQuery(querySql);

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            StringBuilder sb = new StringBuilder( 1024 );

            if(!isHivePartitioned) {
                if ( columnCount > 0 ) {
                    sb.append( "Create table " ).append(table).append( " ( " );
                }
                for ( int i = 1; i <= columnCount; i ++ ) {
                    if ( i > 1 ) sb.append( ", " );

                    int columnType = rsmd.getColumnType(i);
                    String typeName =rsmd.getColumnTypeName(i);
                    String colName = rsmd.getColumnLabel(i);
                    int precision = rsmd.getPrecision(i);
                    int scale = rsmd.getScale(i);

                    String typeInfo = getTranedType(srcType, destType, columnType,typeName,colName,precision,scale);
                    sb.append(" ").append(colName).append(" ").append(typeInfo);
                } // for columns
                sb.append( " ) " );
            } else {
                int partitionColumnCount = dest.getInt("partitionColumnCount");
                List<String> valueList = dest.getList("partitionValueList",String.class);


                if ( columnCount > 0 ) {
                    sb.append( "Create table " ).append(table).append( " ( " );
                }

                for ( int i = 1; i <= columnCount; i ++ ) {
                    if ( i > 1 && i != columnCount-partitionColumnCount+1 ) sb.append( ", " );
                    if(i==columnCount-partitionColumnCount+1) {
                        sb.append( " ) " );
                        sb.append(" PARTITIONED BY (");
                    }

                    int columnType = rsmd.getColumnType(i);
                    String typeName =rsmd.getColumnTypeName(i);
                    String colName = rsmd.getColumnLabel(i);
                    int precision = rsmd.getPrecision(i);
                    int scale = rsmd.getScale(i);

                    String typeInfo = getTranedType(srcType, destType, columnType,typeName,colName,precision,scale);
                    sb.append(" ").append(colName).append(" ").append(typeInfo);

                    //构造hdfswriter所需的columns信息,和其类型保持一致
                } // for columns
                sb.append( " ) " );
            }


            if(destType == DataBaseType.Hive) {
                String lineDelimiter = StringEscapeUtils.escapeJava(dest.getString("lineDelimiter","\n"));
                String fieldDelimiter = StringEscapeUtils.escapeJava(dest.getString("fieldDelimiter", "\t"));
                String fileType = dest.getString("fileType","text");
                if("text".equalsIgnoreCase(fileType)) {
                    fileType = "textfile";
                }
                sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '"
                        + fieldDelimiter + "' LINES TERMINATED BY '" + lineDelimiter
                        + "' STORED AS " + fileType);
            }
            LOG.warn(sb.toString());

            boolean isAutoCommit = destConn.getAutoCommit();

            if(destType != DataBaseType.Hive)
                destConn.setAutoCommit(false);

            destStmt = destConn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            destStmt.executeUpdate(sb.toString());


            if(destType != DataBaseType.Hive) {
                destConn.commit();
                destConn.setAutoCommit(isAutoCommit);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
                try {
                    if(stmt!=null) {
                        stmt.close();
                    }
                    if(destStmt!=null) {
                        stmt.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }

    }

    private static String getColDesc(DataBaseType srcType,DataBaseType destType, ResultSetMetaData rsmd ,int colIndex) throws SQLException {
        //暂时不支持ads,drds,,tddl
        int columnType = rsmd.getColumnType(colIndex);
        String typeName =rsmd.getColumnTypeName(colIndex);
        String colName = rsmd.getColumnLabel(colIndex);
//        if(srcType == DataBaseType.Hive && colName.toLowerCase().startsWith(String.format("%s\\.",.toLowerCase()))) {
//            colName = colName.substring(typeName)
//        }
        int precision = rsmd.getPrecision(colIndex);
        int scale = rsmd.getScale(colIndex);



        return  null;
    }


    private static String getTranedType(DataBaseType srcType,DataBaseType destType,int columnType,String typeName,String colName,int precision,int scale) {
//        System.out.println("==========colName========" + colName);
//        System.out.println("==========typeName========"+typeName);
//        System.out.println("==========precision========"+precision);
//        System.out.println("==========scale========"+scale);
        switch (columnType) {
            case Types.CHAR:
            case Types.NCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                if(precision <=255 ) {
                    //处理varchar<4000
                    if(destType == DataBaseType.Oracle) {
                        return "varchar2(255)";
                    }else
                        return "varchar(255)";
                }else if(precision <=1000 ) {
                    //处理varchar<4000
                    if(destType == DataBaseType.Oracle) {
                        return "varchar2(1000)";
                    }else
                        return "varchar(1000)";
                }
                else if(precision <=4000 ){
                    //处理varchar<4000
                    if(destType == DataBaseType.Oracle) {
                        return "varchar2(4000)";
                    }else
                        return "varchar(4000)";
                }else if(precision <=8000) {
                    if(destType == DataBaseType.Oracle) {
                        return "clob";
                    }else
                        return "varchar(8000)";
                }else {
                    //处理hive string类型，默认为varchar(8000) 或者varchar2(4000)
                    if (srcType == DataBaseType.Hive) {
                        if (destType == DataBaseType.Oracle) {
                            return "varchar2(4000)";
                        } else
                            return "varchar(8000)";
                    } else {
                        switch (destType) {
                            case Oracle:
                            case DB2:
                                return "clob";
                            case SQLServer:
                            case PostgreSQL:
                                return "text";
                            case MySql:
                                return "LONGTEXT";
                            case Hive:
                                return "string";
                            default:
                                return "clob";
                        }
                    }
                }
            case Types.TINYINT:
                if(precision==1&&scale==0) {
                    //boolean
                    switch (destType) {
                        case Oracle:
                            return  "number(1)";
                        case PostgreSQL:
                        case DB2:
                            return "smallint";
                        default:
                            return "tinyint";
                    }
                }

            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                if (destType == DataBaseType.Oracle) {
                    return "number(38)";
                } else
                    return "bigint";

            case Types.FLOAT:
                return "float";

            case Types.REAL:
            case Types.DOUBLE:
                return "double";

            case Types.NUMERIC:
            case Types.DECIMAL:
                if (destType == DataBaseType.Oracle) {
                    return String.format("number(%d,%d)",precision,scale);
                }

                if(destType == DataBaseType.Hive) {
                    //整数
                    if(scale <= 0) {
                        return String.format("BIGINT");
                    }
//                    if(precision <= 0 || precision > 38)
//                        precision = 38;
//
//                    if(scale < 0) {
//                        scale = 0;
//                    }else if(scale > 38 ) {
//                        scale = 38;
//                    }
//                    return String.format("decimal(%d,%d)",precision,scale);

                    //hdfsWriter 不支持 decimal，故转double，可能出现溢出问题。
                    return String.format("DOUBLE");
                }

                return String.format("decimal(%d,%d)",precision,scale);
                // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
            case Types.DATE:
                switch (destType){
                    case SQLServer:
                        return "datetime";
                    case MySql: //year
                        if(typeName.equalsIgnoreCase("year")) {
                            return "year";
                        }
                    default:
                        return "date";
                }

            case Types.TIME:
                switch (destType) {
                    case Oracle:
                        return "timestamp";
                    case SQLServer:
                        return "datetime";
                    default:
                        return "time";
                }

            case Types.TIMESTAMP:
                switch (destType) {
                    case SQLServer:
                        return "datetime";
                    default:
                        return "timestamp";
                }

                //这里使用上有限制，暂时全部当做长度有限制8000的二进制流处理。符合大多数场景。
                //0-2000
                //
                //
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                if(precision>0 && precision<=2000) {
                    switch (destType) {
                        case Oracle:
                            return String.format("raw(%d)",precision);
                        case DB2:
                            return String.format("VARCHAR(%d) FOR BIT DATA",precision);
                        case PostgreSQL:
                            return String.format("bytea(%d)",precision);
                        case Hive:
                            return String.format("string");
                        default:
                            return String.format("varbinary(%d)",precision);
                    }
                }else if(precision>2000 & precision<=8000) {
                    switch (destType) {
                        case Oracle:
                            return "BLOB";
                        case DB2:
                            return String.format("VARCHAR(%d) FOR BIT DATA", precision);
                        case PostgreSQL:
                            return String.format("bytea(%d)", precision);
                        case Hive:
                            return String.format("string");
                        default:
                            return String.format("varbinary(%d)", precision);
                    }
                }else {
                    switch (destType) {
                        case MySql:
                            return "LONGBLOB";
                        case SQLServer:
                            return "IMAGE";
                        case PostgreSQL:
                            return "bytea";
                        case Hive:
                            return String.format("binary");
                        default:
                            return "BLOB";
                    }
                }
            case Types.BOOLEAN:
                switch (destType) {
                    case Oracle:
                        return  "number(1)";
                    case DB2:
                        return "smallint";
                    default:
                        return "tinyint";
                }

                // warn: bit(1) -> Types.BIT 可使用setBoolean
                // warn: bit(>1) -> Types.VARBINARY 可使用setBytes

            case Types.BIT:
                if(precision==1 && srcType==DataBaseType.MySql) {
                    switch (destType) {
                        case Oracle:
                            return  "number(1)";
                        case DB2:
                            return "smallint";
                        default:
                            return "tinyint";
                    }
                }else {
                    switch (destType) {
                        //warn: 有截断危险,unchecked
                        case Oracle:
                            return  "raw(2000)";
                        case DB2:
                            return "CHAR(255) FOR BIT DATA";
                        case PostgreSQL:
                            return "bit varying";
                        case MySql:
                            return "bit(64)";
                        case SQLServer:
                            return "binary(8000)";
                        default:
                            return "BIT";
                    }
                }
            default:
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.UNSUPPORTED_TYPE,
                                String.format(
                                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%s], 字段Java类型:[%d]. 请修改表中该字段的类型或者不同步该字段.",
                                        colName,
                                        typeName,
                                        columnType));
        }
    }


    public static boolean checkInsertPrivilege(DataBaseType dataBaseType, String jdbcURL, String userName, String password, List<String> tableList) {
        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        String insertTemplate = "insert into %s(select * from %s where 1 = 2)";

        boolean hasInsertPrivilege = true;
        Statement insertStmt = null;
        for(String tableName : tableList) {
            String checkInsertPrivilegeSql = String.format(insertTemplate, tableName, tableName);
            try {
                insertStmt = connection.createStatement();
                executeSqlWithoutResultSet(insertStmt, checkInsertPrivilegeSql);
            } catch (Exception e) {
                if(DataBaseType.Oracle.equals(dataBaseType)) {
                    if(e.getMessage() != null && e.getMessage().contains("insufficient privileges")) {
                        hasInsertPrivilege = false;
                        LOG.warn("User [" + userName +"] has no 'insert' privilege on table[" + tableName + "], errorMessage:[{}]", e.getMessage());
                    }
                } else {
                    hasInsertPrivilege = false;
                    LOG.warn("User [" + userName + "] has no 'insert' privilege on table[" + tableName + "], errorMessage:[{}]", e.getMessage());
                }
            }
        }
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.warn("connection close failed, " + e.getMessage());
        }
        return hasInsertPrivilege;
    }

    public static boolean checkDeletePrivilege(DataBaseType dataBaseType,String jdbcURL, String userName, String password, List<String> tableList) {
        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        String deleteTemplate = "delete from %s WHERE 1 = 2";

        boolean hasInsertPrivilege = true;
        Statement deleteStmt = null;
        for(String tableName : tableList) {
            String checkDeletePrivilegeSQL = String.format(deleteTemplate, tableName);
            try {
                deleteStmt = connection.createStatement();
                executeSqlWithoutResultSet(deleteStmt, checkDeletePrivilegeSQL);
            } catch (Exception e) {
                hasInsertPrivilege = false;
                LOG.warn("User [" + userName +"] has no 'delete' privilege on table[" + tableName + "], errorMessage:[{}]", e.getMessage());
            }
        }
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.warn("connection close failed, " + e.getMessage());
        }
        return hasInsertPrivilege;
    }

    public static boolean needCheckDeletePrivilege(Configuration originalConfig) {
        List<String> allSqls =new ArrayList<String>();
        List<String> preSQLs = originalConfig.getList(Key.PRE_SQL, String.class);
        List<String> postSQLs = originalConfig.getList(Key.POST_SQL, String.class);
        if (preSQLs != null && !preSQLs.isEmpty()){
            allSqls.addAll(preSQLs);
        }
        if (postSQLs != null && !postSQLs.isEmpty()){
            allSqls.addAll(postSQLs);
        }
        for(String sql : allSqls) {
            if(StringUtils.isNotBlank(sql)) {
                if (sql.trim().toUpperCase().startsWith("DELETE")) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Get direct JDBC connection
     * <p/>
     * if connecting failed, try to connect for MAX_TRY_TIMES times
     * <p/>
     * NOTE: In DataX, we don't need connection pool in fact
     */
    public static Connection getConnection(final DataBaseType dataBaseType,
                                           final String jdbcUrl, final String username, final String password) {
        return getConnection(dataBaseType, jdbcUrl, username, password, String.valueOf(Constant.SOCKET_TIMEOUT_INSECOND * 1000));
    }

    /**
     *
     * @param dataBaseType
     * @param jdbcUrl
     * @param username
     * @param password
     * @param socketTimeout 设置socketTimeout，单位ms，String类型
     * @return
     */
    public static Connection getConnection(final DataBaseType dataBaseType,
                                           final String jdbcUrl, final String username, final String password, final String socketTimeout) {
        try {
            return RetryUtil.executeWithRetry(new Callable<Connection>() {
                @Override
                public Connection call() throws Exception {
                    return DBUtil.connect(dataBaseType, jdbcUrl, username,
                            password, socketTimeout);
                }
            }, 9, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONN_DB_ERROR,
                    String.format("数据库连接失败. 因为根据您配置的连接信息:%s获取数据库连接失败. 请检查您的配置并作出修改.", jdbcUrl), e);
        }
    }

    /**
     * Get direct JDBC connection
     * <p/>
     * if connecting failed, try to connect for MAX_TRY_TIMES times
     * <p/>
     * NOTE: In DataX, we don't need connection pool in fact
     */
    public static Connection getConnectionWithoutRetry(final DataBaseType dataBaseType,
                                                       final String jdbcUrl, final String username, final String password) {
        return getConnectionWithoutRetry(dataBaseType, jdbcUrl, username,
                password, String.valueOf(Constant.SOCKET_TIMEOUT_INSECOND * 1000));
    }

    public static Connection getConnectionWithoutRetry(final DataBaseType dataBaseType,
                                                       final String jdbcUrl, final String username, final String password, String socketTimeout) {
        return DBUtil.connect(dataBaseType, jdbcUrl, username,
                password, socketTimeout);
    }

    private static synchronized Connection connect(DataBaseType dataBaseType,
                                                   String url, String user, String pass) {
        return connect(dataBaseType, url, user, pass, String.valueOf(Constant.SOCKET_TIMEOUT_INSECOND * 1000));
    }

    private static synchronized Connection connect(DataBaseType dataBaseType,
                                                   String url, String user, String pass, String socketTimeout) {
        //ob10的处理
        if (url.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING) && dataBaseType == DataBaseType.MySql) {
            String[] ss = url.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
            if (ss.length != 3) {
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
            }
            LOG.info("this is ob1_0 jdbc url.");
            user = ss[1].trim() +":"+user;
            url = ss[2];
            LOG.info("this is ob1_0 jdbc url. user="+user+" :url="+url);
        }

        Properties prop = new Properties();
        prop.put("user", user);
        prop.put("password", pass);

        if (dataBaseType == DataBaseType.Oracle) {
            //oracle.net.READ_TIMEOUT for jdbc versions < 10.1.0.5 oracle.jdbc.ReadTimeout for jdbc versions >=10.1.0.5
            // unit ms
            prop.put("oracle.jdbc.ReadTimeout", socketTimeout);
        }

        return connect(dataBaseType, url, prop);
    }

    private static synchronized Connection connect(DataBaseType dataBaseType,
                                                   String url, Properties prop) {
        try {

            Class.forName(dataBaseType.getDriverClassName());
            DriverManager.setLoginTimeout(Constant.TIMEOUT_SECONDS);

            return DriverManager.getConnection(url, prop);
        } catch (Exception e) {
            throw RdbmsException.asConnException(dataBaseType, e, prop.getProperty("user"), null);
        }
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn Database connection .
     * @param sql  sql statement to be executed
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize)
            throws SQLException {
        // 默认3600 s 的query Timeout
        return query(conn, sql, fetchSize, Constant.SOCKET_TIMEOUT_INSECOND);
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn         Database connection .
     * @param sql          sql statement to be executed
     * @param fetchSize
     * @param queryTimeout unit:second
     * @return
     * @throws SQLException
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize, int queryTimeout)
            throws SQLException {
        // make sure autocommit is off

        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);


        stmt.setFetchSize(fetchSize);
        //stmt.setQueryTimeout(queryTimeout);   //对于hive就需要去除这个设置，不然会报错 mothod not　support

        return query(stmt, sql);
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param stmt {@link Statement}
     * @param sql  sql statement to be executed
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Statement stmt, String sql)
            throws SQLException {
        return stmt.executeQuery(sql);
    }

    public static void executeSqlWithoutResultSet(Statement stmt, String sql)
            throws SQLException {
        stmt.execute(sql);
    }

    /**
     * Close {@link ResultSet}, {@link Statement} referenced by this
     * {@link ResultSet}
     *
     * @param rs {@link ResultSet} to be closed
     * @throws IllegalArgumentException
     */
    public static void closeResultSet(ResultSet rs) {
        try {
            if (null != rs) {
                Statement stmt = rs.getStatement();
                if (null != stmt) {
                    stmt.close();
                    stmt = null;
                }
                rs.close();
            }
            rs = null;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void closeDBResources(ResultSet rs, Statement stmt,
                                        Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException unused) {
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException unused) {
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException unused) {
            }
        }
    }

    public static void closeDBResources(Statement stmt, Connection conn) {
        closeDBResources(null, stmt, conn);
    }

    public static List<String> getTableColumns(DataBaseType dataBaseType,
                                               String jdbcUrl, String user, String pass, String tableName) {
        Connection conn = getConnection(dataBaseType, jdbcUrl, user, pass);
        return getTableColumnsByConn(dataBaseType, conn, tableName, "jdbcUrl:"+jdbcUrl);
    }

    public static List<String> getTableColumnsByConn(DataBaseType dataBaseType, Connection conn, String tableName, String basicMsg) {
        List<String> columns = new ArrayList<String>();
        Statement statement = null;
        ResultSet rs = null;
        String queryColumnSql = null;
        try {
            statement = conn.createStatement();
            queryColumnSql = String.format("select * from %s where 1=2",
                    tableName);
            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                columns.add(rsMetaData.getColumnName(i + 1));
            }

        } catch (SQLException e) {
            throw RdbmsException.asQueryException(dataBaseType,e,queryColumnSql,tableName,null);
        } finally {
            DBUtil.closeDBResources(rs, statement, conn);
        }

        return columns;
    }

    /**
     * @return Left:ColumnName Middle:ColumnType Right:ColumnTypeName
     */
    public static Triple<List<String>, List<Integer>, List<String>> getColumnMetaData(
            DataBaseType dataBaseType, String jdbcUrl, String user,
            String pass, String tableName, String column) {
        Connection conn = null;
        try {
            conn = getConnection(dataBaseType, jdbcUrl, user, pass);
            return getColumnMetaData(conn, tableName, column);
        } finally {
            DBUtil.closeDBResources(null, null, conn);
        }
    }

    /**
     * @return Left:ColumnName Middle:ColumnType Right:ColumnTypeName
     */
    public static Triple<List<String>, List<Integer>, List<String>> getColumnMetaData(
            Connection conn, String tableName, String column) {
        Statement statement = null;
        ResultSet rs = null;

        Triple<List<String>, List<Integer>, List<String>> columnMetaData = new ImmutableTriple<List<String>, List<Integer>, List<String>>(
                new ArrayList<String>(), new ArrayList<Integer>(),
                new ArrayList<String>());
        try {
            statement = conn.createStatement();
            String queryColumnSql = "select " + column + " from " + tableName
                    + " where 1=2";

            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {

                columnMetaData.getLeft().add(rsMetaData.getColumnName(i + 1));
                columnMetaData.getMiddle().add(rsMetaData.getColumnType(i + 1));
                columnMetaData.getRight().add(
                        rsMetaData.getColumnTypeName(i + 1));
            }
            return columnMetaData;

        } catch (SQLException e) {
            throw DataXException
                    .asDataXException(DBUtilErrorCode.GET_COLUMN_INFO_FAILED,
                            String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.", tableName), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, null);
        }
    }

    public static boolean testConnWithoutRetry(DataBaseType dataBaseType,
                                               String url, String user, String pass, boolean checkSlave){
        Connection connection = null;

        //LOG.warn("dataBaseType:{},url:{},user:{},pass:{},checkSlave:{}",dataBaseType,url,user,pass,checkSlave);

        try {
            connection = connect(dataBaseType, url, user, pass);
            if (connection != null) {
                if (dataBaseType.equals(dataBaseType.MySql) && checkSlave) {
                    //dataBaseType.MySql
                    boolean connOk = !isSlaveBehind(connection);
                    return connOk;
                } else {
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.warn("test connection of [{}] failed, for {}.", url,
                    e.getMessage());
        } finally {
            DBUtil.closeDBResources(null, connection);
        }
        return false;
    }

    public static boolean testConnWithoutRetry(DataBaseType dataBaseType,
                                               String url, String user, String pass, List<String> preSql) {
        Connection connection = null;
        try {
            connection = connect(dataBaseType, url, user, pass);
            if (null != connection) {
                for (String pre : preSql) {
                    if (doPreCheck(connection, pre) == false) {
                        LOG.warn("doPreCheck failed.");
                        return false;
                    }
                }
                return true;
            }
        } catch (Exception e) {
            LOG.warn("test connection of [{}] failed, for {}.", url,
                    e.getMessage());
        } finally {
            DBUtil.closeDBResources(null, connection);
        }

        return false;
    }

    public static boolean isOracleMaster(final String url, final String user, final String pass) {
        try {
            return RetryUtil.executeWithRetry(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    Connection conn = null;
                    try {
                        conn = connect(DataBaseType.Oracle, url, user, pass);
                        ResultSet rs = query(conn, "select DATABASE_ROLE from V$DATABASE");
                        if (DBUtil.asyncResultSetNext(rs, 5)) {
                            String role = rs.getString("DATABASE_ROLE");
                            return "PRIMARY".equalsIgnoreCase(role);
                        }
                        throw DataXException.asDataXException(DBUtilErrorCode.RS_ASYNC_ERROR,
                                String.format("select DATABASE_ROLE from V$DATABASE failed,请检查您的jdbcUrl:%s.", url));
                    } finally {
                        DBUtil.closeDBResources(null, conn);
                    }
                }
            }, 3, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONN_DB_ERROR,
                    String.format("select DATABASE_ROLE from V$DATABASE failed, url: %s", url), e);
        }
    }

    public static ResultSet query(Connection conn, String sql)
            throws SQLException {
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        //默认3600 seconds
        stmt.setQueryTimeout(Constant.SOCKET_TIMEOUT_INSECOND);
        return query(stmt, sql);
    }

    private static boolean doPreCheck(Connection conn, String pre) {
        ResultSet rs = null;
        try {
            rs = query(conn, pre);

            int checkResult = -1;
            if (DBUtil.asyncResultSetNext(rs)) {
                checkResult = rs.getInt(1);
                if (DBUtil.asyncResultSetNext(rs)) {
                    LOG.warn(
                            "pre check failed. It should return one result:0, pre:[{}].",
                            pre);
                    return false;
                }

            }

            if (0 == checkResult) {
                return true;
            }

            LOG.warn(
                    "pre check failed. It should return one result:0, pre:[{}].",
                    pre);
        } catch (Exception e) {
            LOG.warn("pre check failed. pre:[{}], errorMessage:[{}].", pre,
                    e.getMessage());
        } finally {
            DBUtil.closeResultSet(rs);
        }
        return false;
    }

    // warn:until now, only oracle need to handle session config.
    public static void dealWithSessionConfig(Connection conn,
                                             Configuration config, DataBaseType databaseType, String message) {
        List<String> sessionConfig = null;
        switch (databaseType) {
            case Oracle:
                sessionConfig = config.getList(Key.SESSION,
                        new ArrayList<String>(), String.class);
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            case DRDS:
                // 用于关闭 drds 的分布式事务开关
                sessionConfig = new ArrayList<String>();
                sessionConfig.add("set transaction policy 4");
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            case MySql:
                sessionConfig = config.getList(Key.SESSION,
                        new ArrayList<String>(), String.class);
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            case Hive:
//                sessionConfig = new ArrayList<String>();
//                sessionConfig.add("set ");
//                sessionConfig.add("set hive.server2.async.exec.keepalive.time=60");
//                sessionConfig.add("set hive.server2.long.polling.timeout=15000L");
//                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            default:
                break;
        }
    }

    private static void doDealWithSessionConfig(Connection conn,
                                                List<String> sessions, String message) {
        if (null == sessions || sessions.isEmpty()) {
            return;
        }

        Statement stmt;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            throw DataXException
                    .asDataXException(DBUtilErrorCode.SET_SESSION_ERROR, String
                                    .format("session配置有误. 因为根据您的配置执行 session 设置失败. 上下文信息是:[%s]. 请检查您的配置并作出修改.", message),
                            e);
        }

        for (String sessionSql : sessions) {
            LOG.info("execute sql:[{}]", sessionSql);
            try {
                DBUtil.executeSqlWithoutResultSet(stmt, sessionSql);
            } catch (SQLException e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.SET_SESSION_ERROR, String.format(
                                "session配置有误. 因为根据您的配置执行 session 设置失败. 上下文信息是:[%s]. 请检查您的配置并作出修改.", message), e);
            }
        }
        DBUtil.closeDBResources(stmt, null);
    }

    public static void sqlValid(String sql, DataBaseType dataBaseType){
        SQLStatementParser statementParser = SQLParserUtils.createSQLStatementParser(sql,dataBaseType.getTypeName());
        statementParser.parseStatementList();
    }

    /**
     * 异步获取resultSet的next(),注意，千万不能应用在数据的读取中。只能用在meta的获取
     * @param resultSet
     * @return
     */
    public static boolean asyncResultSetNext(final ResultSet resultSet) {
        return asyncResultSetNext(resultSet, 3600);
    }

    public static boolean asyncResultSetNext(final ResultSet resultSet, int timeout) {
        Future<Boolean> future = rsExecutors.get().submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return resultSet.next();
            }
        });
        try {
            return future.get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.RS_ASYNC_ERROR, "异步获取ResultSet失败", e);
        }
    }
    
    public static void loadDriverClass(String pluginType, String pluginName) {
        try {
            String pluginJsonPath = StringUtils.join(
                    new String[] { System.getProperty("datax.home"), "plugin",
                            pluginType,
                            String.format("%s%s", pluginName, pluginType),
                            "plugin.json" }, File.separator);
            Configuration configuration = Configuration.from(new File(
                    pluginJsonPath));
            List<String> drivers = configuration.getList("drivers",
                    String.class);
            for (String driver : drivers) {
                Class.forName(driver);
            }
        } catch (ClassNotFoundException e) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                    "数据库驱动加载错误, 请确认libs目录有驱动jar包且plugin.json中drivers配置驱动类正确!",
                    e);
        }
    }
}
