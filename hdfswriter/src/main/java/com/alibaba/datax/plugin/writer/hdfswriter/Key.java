package com.alibaba.datax.plugin.writer.hdfswriter;

/**
 * Created by shf on 15/10/8.
 */
public class Key {
    // must have
    public static final String PATH = "path";
    //must have
    public final static String DEFAULT_FS = "defaultFS";
    //must have
    public final static String FILE_TYPE = "fileType";
    // must have
    public static final String FILE_NAME = "fileName";
    // must have for column
    public static final String COLUMN = "column";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String DATE_FORMAT = "dateFormat";
    // must have
    public static final String WRITE_MODE = "writeMode";
    // must have
    public static final String FIELD_DELIMITER = "fieldDelimiter";
    // not must, default UTF-8
    public static final String ENCODING = "encoding";
    // not must, default no compress
    public static final String COMPRESS = "compress";
    // not must, not default \N
    public static final String NULL_FORMAT = "nullFormat";
    // Kerberos
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";




    /*
       * @name:hiveTableName
       * @description:switch to hive table  table name
       * @mandatory: false
       * @default:null
       */
    public final static String hiveTableName = "hive_table_name";
    /*
       * @name:hiveTableSwitch
       * @description:switch to hive table
       * @range:TRUE|FALSE|true|false
       * @mandatory: false
       * @default:false
       */
    public final static String hiveTableSwitch= "hive_table_switch";
    /*
      * @name:hiveUsername
      * @description:hive username
      * @mandatory: false
      * @default:"datax"
      */
    public final static String hiveUsername= "hive_username";


    /*
      * @name:hivePassword
      * @description:hive passwrd
      * @mandatory: false
      * @default:"datax"
      */
    public final static String hivePassword= "hive_password";



    /*
       * @name:partitionNames
       * @description:hive table partition names
       * @mandatory: false
       * @default:""
       */
    public final static String partitionNames = "partition_names";

    /*
     * @name:partitionValues
     * @description:hive table partition values
     * @mandatory: false
     * @default:""
     */
    public final static String partitionValues = "partition_values";

    /*
    * @name:hiveServer
    * @description:hive server ip
    * @mandatory: false
    * @default:"192.168.41.225"
    */
    public final static String hiveServer = "hive_server";

    /*
  * @name:hiveServerPort
  * @description:hive server port
  * @mandatory: false
  * @default:"10000"
  */
    public final static String hiveServerPort = "hive_server_port";
    /*
    * @name:hiveDatabase
    * @description:hive database name
    * @mandatory: false
    * @default:"default"
    */
    public final static String hiveDatabase = "hive_database";


    public static final String CONN_MARK = "connection";
    public static final String TABLE_MARK = "table";
    public static final  String JDBC_URL = "jdbcUrl";
    public static final  String USERNAME = "username";
    public static final  String PASSWORD = "password";

    public static final  String IS_HIVE_PARTITIONED = "isHivePartitioned";
    public static final  String HIVE_PARTITION_COLUMNS_COUNT = "partitionColumnCount";
    public static final  String HIVE_PARTITION_VALUE_LIST = "partitionValueList";

    public static final String TABLE_PATH = "tablePath";





}
