package com.alibaba.datax.common.util;


import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhuhq on 2016/4/6.
 */
public class DecryptUtil {
    private static final  String keyStr = "key for datax500";
    private static final  String charset = "UTF-8";
    private static final Logger logger = LoggerFactory.getLogger(DecryptUtil.class);
    public static String decrypt(String encode) {
        if(StringUtils.isBlank(encode) || encode.length() <= 32) {
            return  encode;
        }
        String decode = "";
        try {
            logger.info("encode:" + encode);
            String iv = encode.substring(0,16);
            byte[] toDecrypt = new byte[32];
            int j = 0;
            int len = encode.length() - 1;
            for(int i = 16; i < len; i+=2) {
                toDecrypt[j] =(byte)Integer.parseInt(encode.substring(i, i + 2), 16);
                j += 1;
            }
            Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
            SecretKeySpec key = new SecretKeySpec(keyStr.getBytes(charset),"AES");
            cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv.getBytes(charset)));
            byte[] result  = cipher.doFinal(toDecrypt);
            decode = new String(result,charset).trim();
            logger.info("decode length:" + decode.length());
        }catch (Exception ex) {
            logger.error("decode error:",ex);
            throw  DataXException.asDataXException(CommonErrorCode.DECRYPT_ERROR,ex);
        }

        return  decode;
    }
}
