package edu.zju.gis.hadoop.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import scala.Tuple2;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Geometry.Type;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;

import edu.zju.gis.hbase.tool.HBaseHelper;



/**
 * 该类为工具类，提供其他类依赖的基本方法，属性
 * @author xiaobao
 *
 */
public class Utils {
    @Deprecated
    public static final int DELIMITER_NUM = 7;//县级行政区划路径深度
    public static String DEFAULT_INPUT_DELIMITER = "\t";//输入文件的默认分隔符
    public static String DEFAULT_OUTPUT_DELIMITER = "\t";//输出文件的默认分隔符


}


