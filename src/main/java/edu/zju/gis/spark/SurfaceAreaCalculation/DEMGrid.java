package edu.zju.gis.spark.SurfaceAreaCalculation;

import com.esri.core.geometry.*;
//import edu.zju.gis.GNCstatistic_zhy.Utils;
import org.gdal.gdal.Band;

import java.io.Serializable;

import static edu.zju.gis.spark.Utils.pixel2World;
import static edu.zju.gis.spark.Utils.world2Pixel;

/**
 * Created by zhy on 2017/8/4.
 */
public class DEMGrid implements Serializable {
    public int Row;
    public int Col;
    public double xCoor;
    public double yCoor;
    private double gridsize;    //如果用int的话记得后面计算的时候要转成double  不然会算错
    private double centerH;   //这个dem格网的中心点高程值
    public double[] GridsHeight;
    public double[] dHeight;
    public int[][] dRowCol;  //8个三角形的行列号变化值
    public double[][] dxy;   //对于角点坐标 其他的八个三角形的坐标值


    public DEMGrid(int row, int col) {
    }

    public DEMGrid(int row, int col, double[] geoTrans, Band band) {
        this.Row = row;
        this.Col = col;
//        double rowSize = geoTrans[1];    //行高
//        double colSize = geoTrans[5];    //列宽
        this.gridsize = Math.abs(geoTrans[1]);
        double[] buf = new double[1];
        band.ReadRaster(this.Col, this.Row, 1, 1, buf);
        this.centerH = buf[0];
        double[] xy = pixel2World(geoTrans, this.Row, this.Col);
        //这个对于dem来说格子是正方形 应该都是一样的
        this.xCoor = xy[0] + gridsize / 2;
        this.yCoor = xy[1] - gridsize / 2;
        //周围的八个高程值赋值
        this.dRowCol = new int[][]{{-1, 0}, {-1, 1}, {0, 1}, {1, 1}, {1, 0}, {1, -1}, {0, -1}, {-1, -1}};
        this.dxy = new double[][]{{-0.5, 0.5}, {0, 0.5}, {0.5, 0.5}, {0.5, 0}, {0.5, -0.5}, {0, -0.5}, {-0.5, -0.5}, {-0.5, 0}};
        this.GridsHeight = new double[8];
        for (int i = 0; i < 8; i++) {
            int r = this.Row + dRowCol[i][0];
            int c = this.Col + dRowCol[i][1];
            if (r < 0 || c < 0 || r >= band.getYSize() || c >= band.getXSize()) {
                this.GridsHeight[i] = this.centerH;
            } else {
                double[] buf2 = new double[1];
                band.ReadRaster(c, r, 1, 1, buf2);
                if (buf2[0] >= 10000 || buf2[0] <= -10000)
                    buf2[0] = 0;
                this.GridsHeight[i] = buf2[0];
            }
        }
        this.dHeight = new double[8];
        for (int i = 0; i < 8; i += 2) {
            this.dHeight[i] = Math.abs(GridsHeight[i] - centerH);
            this.dHeight[i + 1] = Math.abs(GridsHeight[i] - centerH);
        }
    }

    public DEMGrid(int row, int col, double[] geoTrans, float[] buf, int rowCount, int colCount) {
        this.Row = row;
        this.Col = col;
//        double rowSize = geoTrans[1];    //行高
//        double colSize = geoTrans[5];    //列宽
        this.gridsize = Math.abs(geoTrans[1]);    //一个格网的大小
        this.centerH = buf[row * colCount + col];
        double[] xy = pixel2World(geoTrans, this.Row, this.Col);
        //这个对于dem来说格子是正方形 应该都是一样的
        this.xCoor = xy[0] + gridsize / 2;
        this.yCoor = xy[1] - gridsize / 2;
        //周围的八个高程值赋值
        this.dRowCol = new int[][]{{-1, 0}, {-1, 1}, {0, 1}, {1, 1}, {1, 0}, {1, -1}, {0, -1}, {-1, -1}};
        this.dxy = new double[][]{{-0.5, 0.5}, {0, 0.5}, {0.5, 0.5}, {0.5, 0}, {0.5, -0.5}, {0, -0.5}, {-0.5, -0.5}, {-0.5, 0}};
        this.GridsHeight = new double[8];
        for (int i = 0; i < 8; i++) {
            int r = this.Row + dRowCol[i][0];
            int c = this.Col + dRowCol[i][1];
            if (r < 0 || c < 0 || r >= rowCount || c >= colCount) {
                this.GridsHeight[i] = this.centerH;
            } else {
                if (buf[r * colCount + c] >= 10000 || buf[r * colCount + c] <= -10000) {
                    this.GridsHeight[i] = 0;
                } else {
                    this.GridsHeight[i] = buf[r * colCount + c];
                }
            }
        }
        this.dHeight = new double[8];
        for (int i = 0; i < 8; i += 2) {
            this.dHeight[i] = Math.abs(GridsHeight[i] - centerH);
            this.dHeight[i + 1] = Math.abs(GridsHeight[i] - centerH);
        }
    }


    //clipfeat与这个格网相交 计算其相交的面积
    public double calAreaInGrid(Geometry lcraGeo, boolean isFullContain) {
        //将这个格网的8个三角形与该要素做相交  对于相交的三角形 根据边上的三角形信息计算出大三角形的面积 然后计算出要素的面积
        double sum_area = 0;
        if (isFullContain == false) {
            for (int i = 0; i < 8; i++) {
                String triangle_wkt = null;
                if (i == 7) {
                    triangle_wkt = "POLYGON((" + this.xCoor + " " + this.yCoor + "," + (this.xCoor + this.dxy[i][0] * gridsize) + " " + (this.yCoor + this.dxy[i][1] * gridsize) + "," + (this.xCoor + this.dxy[0][0] * gridsize) + " " + (this.yCoor + this.dxy[0][1] * gridsize) + "))";
                } else {
                    triangle_wkt = "POLYGON((" + this.xCoor + " " + this.yCoor + "," + (this.xCoor + this.dxy[i][0] * gridsize) + " " + (this.yCoor + this.dxy[i][1] * gridsize) + "," + (this.xCoor + this.dxy[i + 1][0] * gridsize) + " " + (this.yCoor + this.dxy[i + 1][1] * gridsize) + "))";
                }
                com.esri.core.geometry.Geometry triangle = GeometryEngine.geometryFromWkt(triangle_wkt, WktImportFlags.wktImportDefaults, com.esri.core.geometry.Geometry.Type.Polygon);
                com.esri.core.geometry.Geometry intersect = GeometryEngine.intersect(triangle, lcraGeo, null);

                double dH;
                if (intersect != null) {
                    double intersect_area = intersect.calculateArea2D();
                    if (intersect_area == 0)
                        continue;
                    //计算这个三角形的面积
                    double a = Math.sqrt(gridsize * gridsize + dHeight[i] * dHeight[i]);
                    //b从左上角那个三角形开始算
                    if (i == 0) {
                        dH = Math.abs(GridsHeight[7] - GridsHeight[0]);
                    } else {
                        dH = Math.abs(GridsHeight[i] - GridsHeight[i - 1]);
                    }
                    double b = Math.sqrt(gridsize * gridsize + dH * dH);
                    double triangle_area = (a * b / 2) / 4;
                    double real_area = (intersect_area / (gridsize * gridsize / 8)) * triangle_area;
                    sum_area += real_area;
                }
            }
            return sum_area;
        } else {
            //计算整个矩形的面积
            for (int i = 0; i < 8; i++) {
                String triangle_wkt = null;
                if (i == 7) {
                    triangle_wkt = "POLYGON((" + this.xCoor + " " + this.yCoor + "," + (this.xCoor + this.dxy[i][0] * gridsize) + " " + (this.yCoor + this.dxy[i][1] * gridsize) + "," + (this.xCoor + this.dxy[0][0] * gridsize) + " " + (this.yCoor + this.dxy[0][1] * gridsize) + "))";
                } else {
                    triangle_wkt = "POLYGON((" + this.xCoor + " " + this.yCoor + "," + (this.xCoor + this.dxy[i][0] * gridsize) + " " + (this.yCoor + this.dxy[i][1] * gridsize) + "," + (this.xCoor + this.dxy[i + 1][0] * gridsize) + " " + (this.yCoor + this.dxy[i + 1][1] * gridsize) + "))";
                }
                com.esri.core.geometry.Geometry triangle = GeometryEngine.geometryFromWkt(triangle_wkt, WktImportFlags.wktImportDefaults, com.esri.core.geometry.Geometry.Type.Polygon);
                //计算这个三角形的面积
                double a = Math.sqrt(gridsize * gridsize + dHeight[i] * dHeight[i]);
                double dH;
                //b从左上角那个三角形开始算
                if (i == 0) {
                    dH = Math.abs(GridsHeight[7] - GridsHeight[0]);
                } else {
                    dH = Math.abs(GridsHeight[i] - GridsHeight[i - 1]);
                }
                double b = Math.sqrt(gridsize * gridsize + dH * dH);
                double triangle_area = (a * b / 2) / 4;
                sum_area += triangle_area;
            }
            return sum_area;
        }
    }


    //判断某一格网是否是全包含关系
    //判断原则：如果一个格网的周围8个格网都是相交关系　则这个格网属于全包含格网（有特殊情况　暂时先不考虑　比如比较弯曲的水域就　不太适用）
    //注意　这里参数里面的row和col是相对于ＭＢＲ来说的　　而不是整个ｄｅｍ范围　　所以不能直接用this.row　和 this.col
    public boolean IsFullContain(int[][] demrasters, int row, int col) {
        if (row == 0 || col == 0 || row == demrasters.length - 1 || col == demrasters[0].length - 1) {
            //这些格网属于边缘格网 无法判断是否全包含
            return false;
        } else {
            for (int i = 0; i < 8; i++) {
                int value = demrasters[row + this.dRowCol[i][0]][col + this.dRowCol[i][1]];
                if (value == 1) {
                    continue;
                } else {
                    return false;
                }
            }
            return true;
        }
    }

//    public static double scanLineByBuf(double[] geoTrans, ClipFeature clipFeature, int colNum, int rowNum, float[] buf) {
//        double m_extends[] = new double[4];
//        m_extends[0] = clipFeature.getEnv_xmin();
//        m_extends[1] = clipFeature.getEnv_xmax();
//        m_extends[2] = clipFeature.getEnv_ymin();
//        m_extends[3] = clipFeature.getEnv_ymax();
//        //将左上角和右下角的角点转换成像素坐标
//        int[] ulx_uly = world2Pixel(geoTrans, m_extends[0], m_extends[3]);   //左上角
//        int[] lrx_lry = world2Pixel(geoTrans, m_extends[1], m_extends[2]);   //右下角
//        int rxsize = lrx_lry[0] - ulx_uly[0] + 1;         //这个MBR所占的行数
//        int rysize = lrx_lry[1] - ulx_uly[1] + 1;         //这个MBR所占的列数
//        //TODO 如果这个矢量文件很小 需要判断矢量文件是否超出了栅格范围
//        int[][] demrasters = new int[rxsize][rysize];
////        int[][] demrasters = new int[rowNum][colNum];
//        Geometry lcraGeo = clipFeature.getGeo();
//        //start scanning
//        int xrange = 0;
//        int yrange = 0;
//        if((lrx_lry[0]+1)>rowNum){
//            xrange = rowNum;
//        }else{
//            xrange = lrx_lry[0]+1;
//        }
////        if((lrx_lry[1]+1)>colNum){
////            yrange = colNum;
////        }else{
////            yrange = lrx_lry[1]+1;
////        }
//        for (int x = ulx_uly[0] + 1; x < xrange; x++) {
//            double[] leftPoint = pixel2World(geoTrans, x, ulx_uly[1]);
//            double[] rightPoint = pixel2World(geoTrans, x, lrx_lry[1] + 1);
////            double[] rightPoint = pixel2World(geoTrans, x, yrange);
//            String wktLine = "LINESTRING(" + leftPoint[0] + " " + leftPoint[1] + "," + rightPoint[0] + " " + rightPoint[1] + ")";
//            Geometry lineGeo = GeometryEngine.geometryFromWkt(wktLine, WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
//            Geometry intersect = GeometryEngine.intersect(lineGeo, lcraGeo, null);
//            if (intersect != null) {
//                Polyline multiline = (Polyline) intersect;
//                Point2D[] point2Ds = multiline.getCoordinates2D();
//                if (point2Ds.length == 0)
//                    continue;
//                //TODO 如果nodeCount是奇数
//                if (point2Ds.length % 2 == 0) {
//                    int tupleNum = point2Ds.length / 2;
//                    for (int j = 0; j < tupleNum; j++) {
//                        int[] pl1 = world2Pixel(geoTrans, point2Ds[j * 2].x, point2Ds[j * 2].y);
//                        int[] pl2 = world2Pixel(geoTrans, point2Ds[j * 2 + 1].x, point2Ds[j * 2 + 1].y);
//                        if (pl1[0] <= pl2[0]) {
//                            for (int k = pl1[1]; k <= pl2[1]; k++) {
//                                demrasters[x - 1 - ulx_uly[0]][k - ulx_uly[1]] = 1;
//                                demrasters[x - ulx_uly[0]][k - ulx_uly[1]] = 1;
//                            }
//                        } else {
//                            for (int k = pl2[1]; k <= pl1[1]; k++) {
//                                demrasters[x - 1 - ulx_uly[0]][k - ulx_uly[1]] = 1;
//                                demrasters[x - ulx_uly[0]][k - ulx_uly[1]] = 1;
//                            }
//                        }
//                    }
//                } else {
//                    System.out.println("该扫描线与多边形的交点有 " + point2Ds.length + "个");
//                }
//            }
//        }
//        //现在已经获得了这个要素的对应的栅格   每次计算都去读这个文件？？还是把这个文件读到内存里来？
//        //计算地表面积
//        double featureArea = 0;
//        featureArea = featureAreaCalculationByBuf(demrasters, clipFeature, lcraGeo, geoTrans, buf, ulx_uly[0], ulx_uly[1], rowNum, colNum);
//        System.out.println("=======要素 " + clipFeature.getFeatureID() + " 的表面积为：" + featureArea + "=========");
//        return featureArea;
//    }

    public static double featureAreaCalculationByBuf(int[][] demrasters, ClipFeature clipfeat, Geometry geo, double[] geoTrans, float[] buf, int xmin, int ymin, int rowCount, int colCount) {
        double area = 0;
        for (int i = 0; i < demrasters.length; i++) {
            for (int j = 0; j < demrasters[0].length; j++) {
                if (demrasters[i][j] == 1) {
                    //进一步判断这个格网是否是全包含关系
                    int ss = (i + xmin)*colCount+(j + ymin);
                    if(ss>=rowCount*colCount||ss<0)
                    {
                        //说明这个要素已经超出了所在格网的范围 跳过这块dem
                        System.out.println("===========index out of boundary  "+ss+"=============");
                        System.out.println("i:"+i+" j:"+j+" xmin:"+xmin+" ymin:"+ymin);
                        System.out.println((i + xmin)+" "+(j + ymin)+" "+ rowCount+" "+colCount);
                        System.out.println("featureID is  " +clipfeat.getFeatureID());
                        continue;
                    }
                    DEMGrid demGrid = new DEMGrid(i + xmin, j + ymin, geoTrans, buf, rowCount, colCount);
//                    if (demGrid.IsFullContain(demrasters, i, j)) {
//                        double subArea = demGrid.calAreaInGrid(geo, true);
//                        area += subArea;
//                    } else {
//                        double subArea = demGrid.calAreaInGrid(geo, false);
//                        area += subArea;
//                    }
                    double subArea = demGrid.calAreaInGrid(geo, true);
                    area += subArea;
                }
            }
        }
        return area;
    }

    //修改by zhy
    //之前的算法是如果是全包含的格网，计算整个单元格的面积，如果是半包含的格网，就取出和这些格网相交的部分按照面积比例来计算
    //但是这样计算的效率非常低！将算法修改成  如果这个格网的中心在这个多边形里的话 就把他当做全包含关系
    public static double scanLineByBuf(double[] geoTrans, ClipFeature clipFeature, int colNum, int rowNum, float[] buf) {
        double m_extends[] = new double[4];
        m_extends[0] = clipFeature.getEnv_xmin();
        m_extends[1] = clipFeature.getEnv_xmax();
        m_extends[2] = clipFeature.getEnv_ymin();
        m_extends[3] = clipFeature.getEnv_ymax();
        //将左上角和右下角的角点转换成像素坐标
        int[] ulx_uly = world2Pixel(geoTrans, m_extends[0], m_extends[3]);   //左上角
        int[] lrx_lry = world2Pixel(geoTrans, m_extends[1], m_extends[2]);   //右下角
        int rxsize = lrx_lry[0] - ulx_uly[0] + 1;         //这个MBR所占的行数
        int rysize = lrx_lry[1] - ulx_uly[1] + 1;         //这个MBR所占的列数
        //TODO 如果这个矢量文件很小 需要判断矢量文件是否超出了栅格范围
        int[][] demrasters = new int[rxsize][rysize];
        Geometry lcraGeo = clipFeature.getGeo();
        //start scanning
        int xrange = 0;
        if((lrx_lry[0]+1)>rowNum){
            xrange = rowNum;
        }else{
            xrange = lrx_lry[0]+1;
        }
        for (double x = ulx_uly[0] + 0.5; x < xrange; x++) {
            double[] leftPoint = pixel2World(geoTrans, x, ulx_uly[1]);
            double[] rightPoint = pixel2World(geoTrans, x, lrx_lry[1] + 1);
            String wktLine = "LINESTRING(" + leftPoint[0] + " " + leftPoint[1] + "," + rightPoint[0] + " " + rightPoint[1] + ")";
            Geometry lineGeo = GeometryEngine.geometryFromWkt(wktLine, WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
            Geometry intersect = GeometryEngine.intersect(lineGeo, lcraGeo, null);
            if (intersect != null) {
                Polyline multiline = (Polyline) intersect;
                Point2D[] point2Ds = multiline.getCoordinates2D();
                if (point2Ds.length == 0)
                    continue;
                //TODO 如果nodeCount是奇数  暂时不考虑这个问题
                if (point2Ds.length % 2 == 0) {
                    int tupleNum = point2Ds.length / 2;
                    for (int j = 0; j < tupleNum; j++) {
                        int[] pl1 = world2Pixel(geoTrans, point2Ds[j * 2].x, point2Ds[j * 2].y);
                        int[] pl2 = world2Pixel(geoTrans, point2Ds[j * 2 + 1].x, point2Ds[j * 2 + 1].y);
                        //进一步判断这个格网的中心点是否在该多边形内部
                        double p1x = point2Ds[j * 2].x;
                        double p2x = point2Ds[j * 2 + 1].x;

                        if (pl1[0] <= pl2[0]) {
                            for (int k = pl1[1]; k <= pl2[1]; k++) {
                                double centerX =x;
                                double centerY = k+0.5;
                                double centerCoorY = pixel2World(geoTrans, centerX, centerY)[1];
                                double centerCoorX = pixel2World(geoTrans, centerX, centerY)[0];   //获得这个格网中心点的y值
                                if((centerCoorX<=p1x&&centerCoorX>=p2x)||(centerCoorX<=p2x&&centerCoorX>=p1x)){
                                    //多边形包含该格网中心
                                    demrasters[(int)(x - ulx_uly[0])][k - ulx_uly[1]] = 1;
                                }
                            }
                        } else {
                            for (int k = pl2[1]; k <= pl1[1]; k++) {
                                double centerX =x;
                                double centerY = k+0.5;
                                double centerCoorX = pixel2World(geoTrans, centerX, centerY)[0];   //获得这个格网中心点的y值
                                if((centerCoorX<=p1x&&centerCoorX>=p2x)||(centerCoorX<=p2x&&centerCoorX>=p1x)){
                                    //多边形包含该格网中心
                                    demrasters[(int)(x - ulx_uly[0])][k - ulx_uly[1]] = 1;
                                }
//                                demrasters[(int)(x - 1 - ulx_uly[0])][k - ulx_uly[1]] = 1;
//                                demrasters[(int)(x - ulx_uly[0])][k - ulx_uly[1]] = 1;
                            }
                        }
                    }
                } else {
                    System.out.println("该扫描线与多边形的交点有 " + point2Ds.length + "个");
                }
            }
        }
        //现在已经获得了这个要素的对应的栅格   每次计算都去读这个文件？？还是把这个文件读到内存里来？
        //计算地表面积
        double featureArea = 0;
        featureArea = featureAreaCalculationByBuf(demrasters, clipFeature, lcraGeo, geoTrans, buf, ulx_uly[0], ulx_uly[1], rowNum, colNum);
        System.out.println("=======要素 " + clipFeature.getFeatureID() + " 的表面积为：" + featureArea + "=========");
        return featureArea;
    }

}
