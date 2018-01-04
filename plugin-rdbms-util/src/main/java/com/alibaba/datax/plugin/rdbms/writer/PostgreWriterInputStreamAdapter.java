package com.alibaba.datax.plugin.rdbms.writer;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yuanw on 2015/11/6.
 */
public class PostgreWriterInputStreamAdapter extends InputStream {
    private static final Logger LOG = LoggerFactory.getLogger(PostgreWriterInputStreamAdapter.class);
    private RecordReceiver receiver = null;
   // private  Record receiverRecord = null;

    private int readFlag = 0;
    private int lineCounter = 0;
    /* 列分隔符 */
    private char sep = '\001';//001
    /* 行分隔符 */
    private final char BREAK = '\n';
    /* NULL字面字符*/
    private final String NULL = "\\N";

    private String encoding = "UTF8";

    private Record line = null;
    /* 从line中获取一行数据暂存数组*/
    private byte buffer_1[] = null;

    private StringBuilder lineBuilder = new StringBuilder(1024 * 1024 * 8);


    /* 存放上次余下 */
    private byte[] previous = new byte[1024 * 1024 * 8];
    /* 上次余留数据长度 */
    private int preLen = 0;
    /* 上次余留数据起始偏移 */
    private int preOff = 0;

    protected Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;


    public PostgreWriterInputStreamAdapter(RecordReceiver record,Triple<List<String>, List<Integer>, List<String>> resultSetMetaData) {
        super();
        this.receiver = record;
        this.resultSetMetaData = resultSetMetaData;
        //this.sep = writer.getSep();
        //this.encoding = writer.getEncoding();
    }


    @Override
    public int read(byte[] buff, int off, int len) throws IOException {
        if (buff == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > buff.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        if (readFlag ==-1)
        {

            return -1;
        }


        //System.out.printf(String.format("Len: %d\n", len));
        //LOG.info("read start.-----------------------------------------------");
        int total = 0;
        int read = 0;
        while (len > 0) {

            read = this.fetchLine(buff, off, len);
            if (read < 0) {
                readFlag=-1;
                break;
            }
            off += read;
            len -= read;
            total += read;
        }

        if (total == 0) {
            return (-1);
        }



        return total;
    }

    private void buildString(Record record) {

        lineBuilder.setLength(0);
        String field;
        //LOG.info(String.format("record >> %s", record.toString()));




        for (int i = 0, num = record.getColumnNumber();
             i < num; i++) {

            /*int columnSqltype = this.resultSetMetaData.getMiddle().get(i);

            LOG.info(String.format("Column  >> %s", record.getColumn(i)));

            LOG.info(String.format("Column string >> %s", record.getColumn(i).asString()));*/


            if ( record.getColumn(i).getByteSize() == 0) {
                field = null;
            }else{
                //Column ColumnTran = transformColumnType(record.getColumn(i),columnSqltype);
                field = record.getColumn(i).asString();
            }
            //LOG.info(String.format("field val is  >> %s",field.toString()));

            if (null != field) {
                field = field.replace("\\", "\\\\");
                lineBuilder.append(field);
            } else {
                lineBuilder.append("\\N");
            }
            if (i < num - 1)
                lineBuilder.append(this.sep);
            else {
                lineBuilder.append(this.BREAK);
            }

        }
       // LOG.info(String.format("lineBuilder >> %s",lineBuilder.toString()));
    }






    private int fetchLine(byte[] buff, int off, int len) throws UnsupportedEncodingException {
        //LOG.info("this is fetchLine.-----------------------------------------------");
        /* it seems like I am doing C coding. */
        int ret = 0;
        /* record current fetch len */
        int currLen;

        /* 查看上次是否有剩余 */
        if (this.preLen > 0) {
            currLen = Math.min(this.preLen, len);
            System.arraycopy(this.previous, this.preOff, buff, off, currLen);
            this.preOff += currLen;
            this.preLen -= currLen;
            off += currLen;
            len -= currLen;
            ret += currLen;

            /* 如果buff比较小，上次余下的数据 */
            if (this.preLen > 0) {
                return ret;
            }
        }



        /* 本次读数据的逻辑 */
        int lineLen;
        int lineOff = 0;
        line = this.receiver.getFromReader();

        /* line为空，表明数据已全部读完 */
        if (line == null) {
            if (ret == 0)
                return (-1);
            return ret;
        }

        this.lineCounter++;
        this.buildString(line);
        this.buffer_1 = lineBuilder.toString().getBytes(this.encoding);
        lineLen = this.buffer_1.length;
        currLen = Math.min(lineLen, len);
        System.arraycopy(this.buffer_1, 0, buff, off, currLen);
        len -= currLen;
        lineOff +=currLen;
        lineLen -= currLen;
        ret += currLen;
        /* len > 0 表明这次fetchLine还没有将buff填充完毕, buff有剩佄1�7 留作下次填充 */
        if (len > 0) {
            return ret;
        }

        /* 该buffer已经不够放一个line，因此把line的内容保存下来，供下丄1�7次fetch使用
         * 这里的假设是previous足够处1�7 绝对够容纳一个line的内宄1�7 */
        /* fix bug: */
        if (lineLen > this.previous.length) {
            LOG.info("lineLen more than previous>######-------------------------");
            this.previous = new byte[lineLen << 1];
        }
        System.arraycopy(this.buffer_1, lineOff, this.previous, 0, lineLen);
        this.preOff = 0;
        this.preLen = lineLen;
        return (ret);
    }

    @Override
    public int read() throws IOException {
        /*
         * 注意: 没有实现read()
         * */
        throw new IOException("Read() is not supported");
    }

    /*public int getLineNumber() {
        return this.lineCounter;
    }*/
}
