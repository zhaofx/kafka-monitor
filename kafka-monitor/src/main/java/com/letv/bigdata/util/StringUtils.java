package com.letv.bigdata.util;

import java.io.File;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class StringUtils {
    /**
     *  检查字符串是否为null（""视为null）
     * @param text String
     * @return String
     */
    public static String checkNull (String text) {
        if (text == null) {
            return null;
        }
        text = text.trim ();

        if (text.length () > 0) {
            return text;
        } else {
            return null;
        }
    }

    /**
     * 求四舍五入的方法；
     *
     * @param x
     *            需要进行四舍五入运算的数字
     * @param scale
     *            四舍五入到小数点后的位数
     * @return String
     */
    public String getRound (Double x, int scale) {
        NumberFormat format = NumberFormat.getNumberInstance ();
        String result = "";
        format.setMaximumFractionDigits (scale); // 设置小数的最大位数，显示的结果是四舍五入后的
        result = format.format (x).replace ("," , "");
        DecimalFormat df2 = ( DecimalFormat ) NumberFormat.getInstance ();
        df2.applyPattern (getPattern (scale));
        return df2.format (Double.valueOf (result));
    }

    /**
     * 将字符串类型的数字（带有小数点的字符串），按特定的精度处理。
     *
     * @param value 数字
     * @param scale 精度
     * @return String
     */
    public String getFieldValueByPattern (String value, int scale) {
        DecimalFormat df2 = ( DecimalFormat ) NumberFormat.getInstance ();
        df2.applyPattern (getPattern (scale));
        return df2.format (Double.valueOf (value));
    }
    /**
     * getPattern
     * @param scale scale
     * @return String
     */
    public String getPattern (int scale) {

        String pattern = "0.";
        if (scale == 0) {
            pattern = "0";
            return pattern;
        }
        String pad = "0";
        for (int i = 0; i < scale; i++) {
            pattern = pattern + pad;
        }
        return pattern;
    }
    /**
     * lpad
     * @param rString rString
     * @param rLength rLength
     * @return String
     */
    public String lpad (String rString, int rLength) {
        return lpad (rString , rLength , null);
    }

    /**
     * 左填充的方法
     *
     * @param rString
     *            准备被填充的字符串
     * @param rLength
     *            填充之后的字符串长度，也就是该函数返回的字符串长度，如果这个数量比原字符串的长度要短，lpad函数将会把字符串截取成从左到右的n个字符
     *            ;
     * @param rPad
     *            填充字符串，是个可选参数，这个字符串是要粘贴到string的左边，如果这个参数未写，lpad函数将会在string的左边粘贴空格
     *            。
     * @return String
     */
    public static String lpad (String rString, int rLength, String rPad) {
        String lTmpPad = "";
        if (rPad == null) { // 默认用" "空格填充字符串
            rPad = " ";
        }
        String result = "";
        if (!(rString == null || "".equals (rString.trim ()))) { // 如果rString为null，则返回
            // '' 空字符串；
            result = rString;
        }
        if (result.length () >= rLength) {
            return result.substring (0 , rLength);
        } else {
            for (int gCnt = 1; gCnt < rLength - result.length (); gCnt++) {
                lTmpPad = lTmpPad + rPad;
            }
        }
        return lTmpPad + result;
    }
    /**
     * rpad
     * @param rString rString
     * @param rLength rLength
     * @return String
     */
    public static String rpad (String rString, int rLength) {
        return rpad (rString , rLength , null);
    }

    /**
     * 右填充的方法
     *
     * @param rString rString
     * @param rLength rString
     * @param rPad rPad
     * @return String
     */
    public static String rpad (String rString, int rLength, String rPad) {
        String lTmpPad = "";
        if (rPad == null) { // 默认用" "空格填充字符串
            rPad = " ";
        }
        String result = "";
        if (!(rString == null || "".equals (rString.trim ()))) { // 如果rString为null，则返回
            // '' 空字符串；
            result = rString;
        }
        if (result.length () >= rLength) {
            return result.substring (0 , rLength);
        } else {
            for (int gCnt = 1; gCnt < rLength - result.length (); gCnt++) {
                lTmpPad = lTmpPad + rPad;
            }
        }
        return result + lTmpPad;
    }
    /**
     * 根据文件全路径取出文件名
     * @param totalFileName totalFileName
     * @return String
     */
    public static String getSubFileName (String totalFileName) {
        String fileName = null;
        if (totalFileName == null) {
            return "";
        }
        if (totalFileName.lastIndexOf (File.separator) >= 0) {
            fileName = totalFileName.substring (totalFileName.lastIndexOf (File.separator) + 1);
            return fileName;
        } else if (totalFileName.lastIndexOf ("/") >= 0) {
            fileName = totalFileName.substring (totalFileName.lastIndexOf ("/") + 1);
            return fileName;
        } else if (totalFileName.lastIndexOf ("\\") >= 0) {
            fileName = totalFileName.substring (totalFileName.lastIndexOf ("\\") + 1);
            return fileName;
        } else {
            return totalFileName;
        }
    }
    /**
     * 根据文件全路径取出文件路径
     * @param totalFileName totalFileName
     * @return String
     */
    public static String getSubFileDir (String totalFileName) {
        String fileDir = null;
        if (totalFileName == null) {
            return "";
        }
        if (totalFileName.lastIndexOf (File.separator) >= 0) {
            fileDir = totalFileName.substring (0 , totalFileName.lastIndexOf (File.separator));
            return fileDir;
        } else if (totalFileName.lastIndexOf ("/") >= 0) {
            fileDir = totalFileName.substring (0 , totalFileName.lastIndexOf ("/"));
            return fileDir;
        } else {
            return totalFileName;
        }
    }
}
