package com.alibaba.datax.plugin.rdbms.writer.util;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.plugin.rdbms.util.*;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class OriginalConfPretreatmentUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(OriginalConfPretreatmentUtil.class);

    public static DataBaseType DATABASE_TYPE;

//    public static void doPretreatment(Configuration originalConfig) {
//        doPretreatment(originalConfig,null);
//    }

    public static void doPretreatment(Configuration originalConfig, DataBaseType dataBaseType) {
        // 检查 username/password 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME, DBUtilErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD, DBUtilErrorCode.REQUIRED_VALUE);

        doCheckBatchSize(originalConfig);

        simplifyConf(originalConfig);

        dealColumnConf(originalConfig);
        dealWriteMode(originalConfig, dataBaseType);
    }

    public static void doPretreatment(Configuration originalConfig, DataBaseType dataBaseType,Configuration peerConfig,String peerPluginName) {
        // 检查 username/password 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME, DBUtilErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD, DBUtilErrorCode.REQUIRED_VALUE);

        doCheckBatchSize(originalConfig);

        simplifyConf(originalConfig);

        tryCreateDestTable(originalConfig,dataBaseType,peerConfig,peerPluginName);

        if(dataBaseType != DataBaseType.Hive){ //hiveWriter/HDFSWriter的整合 指定了columns为*，不做任何检查 {
            dealColumnConf(originalConfig);
            dealWriteMode(originalConfig, dataBaseType);
        }
    }


    public static void tryCreateDestTable(Configuration dest,DataBaseType destType,Configuration src,String srcPluginName){
        boolean autoCreateTable = dest.getBool("autoCreateTable",true);
        boolean autoDropTable = dest.getBool("autoDropTable",false);

        LOG.warn("autoCreateTable set to {}",autoCreateTable);
        LOG.warn("autoDropTable set to {}",autoDropTable);

        //hive 需要处理分区，path等信息，这里不返回。
        if(destType != DataBaseType.Hive && autoCreateTable == false)
            return;

        //hive 非自动建表，需保证columns和path等信息,这里直接返回，退回hdfswriter处理。
        if(destType == DataBaseType.Hive && autoCreateTable == false) {
            return;
        }

        //原表非rdbms，特殊处理
        Set<String> rdbms = Sets.newHashSet("mysqlreader", "oraclereader", "sqlserverreader","postgresqlreader","drdsreader","hivereader");
        String srcPluginNameLow  = srcPluginName.toLowerCase();
        if(!rdbms.contains(srcPluginNameLow)) {
            if("mongodbreader".equals(srcPluginNameLow)) {
                List<Object> columns = src.getList("column");
                for(int i=0;i<columns.size();i++) {
                    Configuration column = Configuration.from(JSON.toJSONString(columns.get(i)));
                    if("Long".equalsIgnoreCase(column.getString("type"))) {
                        column.set("type","Long");
                    }else if("array".equalsIgnoreCase(column.getString("type"))) {
                        column.set("type","Long");
                    }else if("bytes".equalsIgnoreCase(column.getString("type"))) {
                        column.set("type","Long");
                    }
                    dest.set(String.format("column[%d]",i),column);
                }
            }else {
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, String.format(
                        "Writer对%s的autoCreateTable暂时不支持",srcPluginName));
            }
            return;
        }

        String srcUsername = src.getString(com.alibaba.datax.plugin.rdbms.reader.Key.USERNAME);
        String srcPassword = src.getString(com.alibaba.datax.plugin.rdbms.reader.Key.PASSWORD);
        Configuration srcConnectionConf = Configuration.from(src.getList(com.alibaba.datax.plugin.rdbms.reader.Constant.CONN_MARK, Object.class).get(0).toString());
        String srcJdbcUrl = srcConnectionConf.getString(com.alibaba.datax.plugin.rdbms.reader.Key.JDBC_URL);
        ClassLoader readerLoader = LoadUtil.getJarLoader(PluginType.READER, srcPluginName);
        DataBaseType srcType = DataBaseType.getDataBaseType(srcPluginName); // writer的class loader

        Connection srcConnection = null;
        try {
            //以reader classload加载类
            Class readDBUtil = readerLoader.loadClass("com.alibaba.datax.plugin.rdbms.util.DBUtil");
            Class dataBaseType = readerLoader.loadClass("com.alibaba.datax.plugin.rdbms.util.DataBaseType");
            Method getReadSrcType = dataBaseType.getMethod("getDataBaseType",String.class) ;
            Object readSrcType = getReadSrcType.invoke(null,srcPluginName); //使用的是reader的加载器。
            Method getConnection = readDBUtil.getMethod("getConnection",dataBaseType,String.class,String.class,String.class);

            //ClassLoader origin = Thread.currentThread().getContextClassLoader();
            //Thread.currentThread().setContextClassLoader(readerLoader);
            srcConnection = (Connection) getConnection.invoke(null, readSrcType, srcJdbcUrl,srcUsername,srcPassword);
            //Thread.currentThread().setContextClassLoader(origin);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        String username = dest.getString(Key.USERNAME);
        String password = dest.getString(Key.PASSWORD);
        List<Object> connections = dest.getList(Constant.CONN_MARK,
                Object.class);

        boolean isSrcTableMode = OriginalConfPretreatmentUtil.recognizeTableOrQuerySqlMode(src);

        if(isSrcTableMode) {
            String srcTable = srcConnectionConf.getList(com.alibaba.datax.plugin.rdbms.reader.Key.TABLE, String.class).get(0);
            String columns = src.getString(com.alibaba.datax.plugin.rdbms.reader.Key.COLUMN);

            for (int i = 0, len = connections.size(); i < len; i++) {
                Configuration connConf = Configuration.from(JSON.toJSONString(connections.get(i)));
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                List<String> expandedTables = connConf.getList(Key.TABLE, String.class);

                Connection connection = DBUtil.getConnection(destType,jdbcUrl,username,password) ;

                for(String table:expandedTables) {
                    boolean tableExist =  DBUtil.checkTableExist(connection,table,destType);
                    LOG.warn("dest table {} existence:{}",table,tableExist);

                    if(autoDropTable && tableExist) {
                        LOG.warn("try drop dest table {}",table);
                        DBUtil.dropTable(connection,destType,table);
                    }
                    //DBUtil.checkTableExist(destType,jdbcUrl,username,password,table);

                    if(!tableExist || autoDropTable) {
                        LOG.warn("try create dest table {}",table);
                        DBUtil.createTableWithConfig(srcConnection, srcType, srcTable, connection, destType, table, columns,dest);
                    }

                    //这里表已经建好
                    //boolean isHivePartitionTable = dest.getBool("isHivePartitioned",false);
                    if(destType == DataBaseType.Hive) {
                        //出来hive特有的任务
                        //1. 是否分区及分区partition字段
                        //2. 所有字段名称和类型
                        //3. 分区对应的path 等路径
                        DBUtil.dealHive(dest, connection, table);
                    }
                }
                try {
                    if(connection !=null && !connection.isClosed())
                        connection.close();
                }catch (Exception e) {
                    LOG.warn("connection is closed {}",e);
                }
            }

            try {
                if(srcConnection !=null && !srcConnection.isClosed())
                    srcConnection.close();
            }catch (Exception e) {
                LOG.warn("connection is closed {}",e);
            }
        }else{
            String query = srcConnectionConf.getList(com.alibaba.datax.plugin.rdbms.reader.Key.QUERY_SQL,String.class).get(0);
            for (int i = 0, len = connections.size(); i < len; i++) {
                Configuration connConf = Configuration.from(JSON.toJSONString(connections.get(i)));
                String jdbcUrl = connConf.getString(Key.JDBC_URL);

                List<String> expandedTables = connConf.getList(Key.TABLE, String.class);
                Connection connection = DBUtil.getConnection(destType,jdbcUrl,username,password) ;

                for(String table:expandedTables) {
                    boolean tableExist =  DBUtil.checkTableExist(connection,table,destType);
                    //DBUtil.checkTableExist(destType,jdbcUrl,username,password,table);
                    LOG.warn("dest table {} existence:{}",table,tableExist);

                    if(autoDropTable && tableExist) {
                        LOG.warn("try drop dest table {}",table);
                        DBUtil.dropTable(connection,destType,table);
                    }
                    if(!tableExist || autoDropTable) {
                        LOG.warn("try create dest table {}",table);
                        DBUtil.createTableWithConfig(srcConnection, srcType, query, connection, destType, table, dest);
                    }
                    //这里表已经建好
                    if(destType == DataBaseType.Hive) {
                        DBUtil.dealHive(dest, connection, table);
                    }
                }
                try {
                    if(connection !=null && !connection.isClosed())
                        connection.close();
                }catch (Exception e) {
                    LOG.warn("connection is closed {}",e);
                }
            }

            try {
                if(srcConnection !=null && !srcConnection.isClosed())
                    srcConnection.close();
            }catch (Exception e) {
                LOG.warn("connection is closed {}",e);
            }
        }
    }



    public static void doPretreatReader(Configuration originalConfig){
        originalConfig.getNecessaryValue(com.alibaba.datax.plugin.rdbms.reader.Key.USERNAME,
                DBUtilErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(com.alibaba.datax.plugin.rdbms.reader.Key.PASSWORD,
                DBUtilErrorCode.REQUIRED_VALUE);

        String where = originalConfig.getString(com.alibaba.datax.plugin.rdbms.reader.Key.WHERE, null);
        if(StringUtils.isNotBlank(where)) {
            String whereImprove = where.trim();
            if(whereImprove.endsWith(";") || whereImprove.endsWith("；")) {
                whereImprove = whereImprove.substring(0,whereImprove.length()-1);
            }
            originalConfig.set(com.alibaba.datax.plugin.rdbms.reader.Key.WHERE, whereImprove);
        }
        simplifyConf(originalConfig);
    }

    public static void doCheckBatchSize(Configuration originalConfig) {
        // 检查batchSize 配置（选填，如果未填写，则设置为默认值）
        int batchSize = originalConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
        if (batchSize < 1) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE, String.format(
                    "您的batchSize配置有误. 您所配置的写入数据库表的 batchSize:%s 不能小于1. 推荐配置范围为：[100-1000], 该值越大, 内存溢出可能性越大. 请检查您的配置并作出修改.",
                    batchSize));
        }

        originalConfig.set(Key.BATCH_SIZE, batchSize);
    }

    public static void simplifyConf(Configuration originalConfig) {
        List<Object> connections = originalConfig.getList(Constant.CONN_MARK,
                Object.class);

        int tableNum = 0;

        for (int i = 0, len = connections.size(); i < len; i++) {
            Configuration connConf = Configuration.from(JSON.toJSONString(connections.get(i)));

            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(jdbcUrl)) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE, "您未配置的写入数据库表的 jdbcUrl.");
            }

            jdbcUrl = DATABASE_TYPE.appendJDBCSuffixForReader(jdbcUrl);
            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.JDBC_URL),
                    jdbcUrl);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            if (null == tables || tables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                        "您未配置写入数据库表的表名称. 根据配置DataX找不到您配置的表. 请检查您的配置并作出修改.");
            }

            // 对每一个connection 上配置的table 项进行解析
            List<String> expandedTables = TableExpandUtil
                    .expandTableConf(DATABASE_TYPE, tables);

            if (null == expandedTables || expandedTables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        "您配置的写入数据库表名称错误. DataX找不到您配置的表，请检查您的配置并作出修改.");
            }

            tableNum += expandedTables.size();

            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK,
                    i, Key.TABLE), expandedTables);
        }

        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
    }

    public static void dealColumnConf(Configuration originalConfig, ConnectionFactory connectionFactory, String oneTable) {
        List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);
        if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    "您的配置文件中的列配置信息有误. 因为您未配置写入数据库表的列名称，DataX获取不到列信息. 请检查您的配置并作出修改.");
        } else {
            boolean isPreCheck = originalConfig.getBool(Key.DRYRUN, false);
            List<String> allColumns;
            if (isPreCheck){
                allColumns = DBUtil.getTableColumnsByConn(DATABASE_TYPE,connectionFactory.getConnecttionWithoutRetry(), oneTable, connectionFactory.getConnectionInfo());
            }else{
                allColumns = DBUtil.getTableColumnsByConn(DATABASE_TYPE,connectionFactory.getConnecttion(), oneTable, connectionFactory.getConnectionInfo());
            }

            LOG.info("table:[{}] all columns:[\n{}\n].", oneTable,
                    StringUtils.join(allColumns, ","));

            if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                LOG.warn("您的配置文件中的列配置信息存在风险. 因为您配置的写入数据库表的列为*，当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。请检查您的配置并作出修改.");

                // 回填其值，需要以 String 的方式转交后续处理
                originalConfig.set(Key.COLUMN, allColumns);
            } else if (userConfiguredColumns.size() > allColumns.size()) {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                        String.format("您的配置文件中的列配置信息有误. 因为您所配置的写入数据库表的字段个数:%s 大于目的表的总字段总个数:%s. 请检查您的配置并作出修改.",
                                userConfiguredColumns.size(), allColumns.size()));
            } else {
                // 确保用户配置的 column 不重复
                ListUtil.makeSureNoValueDuplicate(userConfiguredColumns, false);

                // 检查列是否都为数据库表中正确的列（通过执行一次 select column from table 进行判断）
                DBUtil.getColumnMetaData(connectionFactory.getConnecttion(), oneTable,StringUtils.join(userConfiguredColumns, ","));
            }
        }
    }

    public static void dealColumnConf(Configuration originalConfig) {
        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL));

        String username = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);
        String oneTable = originalConfig.getString(String.format(
                "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE));

        JdbcConnectionFactory jdbcConnectionFactory = new JdbcConnectionFactory(DATABASE_TYPE, jdbcUrl, username, password);
        dealColumnConf(originalConfig, jdbcConnectionFactory, oneTable);
    }

    public static void dealWriteMode(Configuration originalConfig, DataBaseType dataBaseType) {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);

        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL, String.class));

        // 默认为：insert 方式
        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");

        List<String> valueHolders = new ArrayList<String>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            valueHolders.add("?");
        }

        boolean forceUseUpdate = false;
        //ob10的处理
        if (dataBaseType == DataBaseType.MySql && isOB10(jdbcUrl)) {
            forceUseUpdate = true;
        }

        String writeDataSqlTemplate = WriterUtil.getWriteTemplate(columns, valueHolders, writeMode,dataBaseType, forceUseUpdate);

        LOG.info("Write data [\n{}\n], which jdbcUrl like:[{}]", writeDataSqlTemplate, jdbcUrl);

        originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
    }

    public static boolean isOB10(String jdbcUrl) {
        //ob10的处理
        if (jdbcUrl.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING)) {
            String[] ss = jdbcUrl.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
            if (ss.length != 3) {
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
            }
            return true;
        }
        return false;
    }


    /**
     * 为了自动建表添加，需知道是否是查询语句还是table
     * @param originalConfig
     * @return
     */
    public static boolean recognizeTableOrQuerySqlMode(
            Configuration originalConfig) {
        List<Object> conns = originalConfig.getList(com.alibaba.datax.plugin.rdbms.reader.Constant.CONN_MARK,
                Object.class);

        List<Boolean> tableModeFlags = new ArrayList<Boolean>();
        List<Boolean> querySqlModeFlags = new ArrayList<Boolean>();

        String table = null;
        String querySql = null;

        boolean isTableMode = false;
        boolean isQuerySqlMode = false;
        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration connConf = Configuration
                    .from(conns.get(i).toString());
            table = connConf.getString(com.alibaba.datax.plugin.rdbms.reader.Key.TABLE, null);
            querySql = connConf.getString(com.alibaba.datax.plugin.rdbms.reader.Key.QUERY_SQL, null);

            isTableMode = StringUtils.isNotBlank(table);
            tableModeFlags.add(isTableMode);

            isQuerySqlMode = StringUtils.isNotBlank(querySql);
            querySqlModeFlags.add(isQuerySqlMode);

            if (false == isTableMode && false == isQuerySqlMode) {
                // table 和 querySql 二者均未配制
                throw DataXException.asDataXException(
                        DBUtilErrorCode.TABLE_QUERYSQL_MISSING, "您的配置有误. 因为table和querySql应该配置并且只能配置一个. 请检查您的配置并作出修改.");
            } else if (true == isTableMode && true == isQuerySqlMode) {
                // table 和 querySql 二者均配置
                throw DataXException.asDataXException(DBUtilErrorCode.TABLE_QUERYSQL_MIXED,
                        "您的配置凌乱了. 因为datax不能同时既配置table又配置querySql.请检查您的配置并作出修改.");
            }
        }

        // 混合配制 table 和 querySql
        if (!ListUtil.checkIfValueSame(tableModeFlags)
                || !ListUtil.checkIfValueSame(tableModeFlags)) {
            throw DataXException.asDataXException(DBUtilErrorCode.TABLE_QUERYSQL_MIXED,
                    "您配置凌乱了. 不能同时既配置table又配置querySql. 请检查您的配置并作出修改.");
        }

        return tableModeFlags.get(0);
    }

}
