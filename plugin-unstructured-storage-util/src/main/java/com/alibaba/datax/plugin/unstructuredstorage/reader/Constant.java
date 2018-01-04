package com.alibaba.datax.plugin.unstructuredstorage.reader;

public class Constant {
	public static final String DEFAULT_ENCODING = "UTF-8";

	public static final char DEFAULT_FIELD_DELIMITER = ',';

	public static final boolean DEFAULT_SKIP_HEADER = false;

	public static final String DEFAULT_NULL_FORMAT = "\\N";
	
    public static final Integer DEFAULT_BUFFER_SIZE = 8192;

    // add by sunwq ,用于非结构化writer是否进行base64编码，用于规避特殊字符
    public static final boolean BASE64_ENCODING = false;
}
