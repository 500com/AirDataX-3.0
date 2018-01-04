package com.alibaba.datax.plugin.reader.hivereader;



import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
//import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

/**
 * Created by luwc on 2017/6/7.
 */
    public class HiveReader extends Reader {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.Hive;


    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;
        /*private static String hiveServerIp="192.168.41.225";
        private static String hiveServerPort="10000";
        private static String database="default";
        private static String tables;
        private static String username="";
        private static String password="";
        private static String where="";
        private static String columns="*";
        private static String sql = "";
        private static String concurrency="1";
        private static Connection conn;*/

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            /*this.hiveServerIp = originalConfig.getString(KeyConstant.ip, hiveServerIp);
            this.hiveServerPort=originalConfig.getString(KeyConstant.port, hiveServerPort);
            this.database = originalConfig.getString(KeyConstant.dbname, database);
            this.tables = originalConfig.getString(KeyConstant.tables, "");
            this.username = originalConfig.getString(KeyConstant.username, username);
            this.password = originalConfig.getString(KeyConstant.password, password);
            this.where = originalConfig.getString(KeyConstant.where, where);
            this.columns = originalConfig.getString(KeyConstant.columns, columns);
            this.concurrency = originalConfig.getString(KeyConstant.concurrency, concurrency);
            this.sql = originalConfig.getString(KeyConstant.sql, sql);*/

            dealFetchSize(this.originalConfig);

            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.originalConfig);

        }

        @Override
        public void preCheck(){
            init();
            this.commonRdbmsReaderJob.preCheck(this.originalConfig,DATABASE_TYPE);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.commonRdbmsReaderJob.split(this.originalConfig, adviceNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }

        private void dealFetchSize(Configuration originalConfig) {
            int fetchSize = originalConfig.getInt(
                    Constant.FETCH_SIZE,
                    10000);
            if (fetchSize < 1) {
                throw DataXException
                        .asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                                String.format("您配置的 fetchSize 有误，fetchSize:[%d] 值不能小于 1.",
                                        fetchSize));
            }
            originalConfig.set(
                    Constant.FETCH_SIZE,
                    fetchSize);
        }

    }

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE,super.getTaskGroupId(), super.getTaskId());
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);

        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig.getInt(Constant.FETCH_SIZE);
            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector(), fetchSize);

        }

        @Override
        public void post() {
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }

    }

}
