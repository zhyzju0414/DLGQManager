package edu.zju.gis.hbase.tool;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


import scala.Tuple2;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;

import edu.zju.gis.hbase.query.querycondition.SpatialQueryCondition;
import edu.zju.gis.hbase.tool.Utils.StatisticIndexEnum;

public class MeasureClass {

    public static final double fa = 6378137.0;
    public static final double fb = 6356752.3141403561;
    public static final double e2 = 0.0066943800229008;
    public static final double ec2 =  0.0067394967754790;
    public static double e;
    public static double A;
    public static double B;
    public static double C;
    public static double D;
    public static double E;

    static{
        e = (fa*fa-fb*fb)/(fa*fa);
        A = 1 + 0.5 * e2 + (3.0 / 8) * e2 * e2 + (35.0 / 112) * e2 * e2 * e2 + (630.0 / 2304) * Math.pow(e2, 4);
        B = (1.0 / 6) * e2 + (15.0 / 80) * e2 * e2 + (21.0 / 112) * e2 * e2 * e2 + (420.0 / 2304) * Math.pow(e2, 4);
        C = (3.0 / 80) * e2 * e2 + (7.0 / 112) * e2 * e2 * e2 + (180.0 / 2304) * Math.pow(e2, 4);
        D = (1.0 / 112) * e2 * e2 * e2 + (45.0 / 2304) * Math.pow(e2, 4);
        E = (5.0 / 2304) * Math.pow(e2, 4);
    }


    public static double CrossMetersEx(double L1, double B1, double L2, double B2, int classMeter)
    {
        double firstCross = CrossMeters(L1, B1, L2, B2);
        double distT = (firstCross / classMeter);
        int distClass = (int)Math.round(distT);

        //start with L1,B1
        double deltaL = (L2 - L1) / distClass;
        double deltaB = (B2 - B1) / distClass;
        double totalDist = 0;

        for (int i = 0; i < distClass; i++)
        {
            double dist = CrossMeters(L1 + i * deltaL, B1 + i * deltaB, L1 + deltaL + i * deltaL, B1 + deltaB + i * deltaB);
            totalDist += dist;
        }
        return totalDist;
    }

    public static double CrossMeters(double L1, double B1, double L2, double B2)
    {
        double result = 0;

        double c = (long)6378137 * (long)6378137 / 6356752.3141403561;
        //double e = Math.Sqrt((long)6378137 * (long)6378137 - 6356752.3141403561 * 6356752.3141403561) / 6356752.3141403561;
        double deltaB = (B2 - B1);//Math.abs
        double deltaL = (L2 - L1);
        double Bm = (B1 + B2) / 2;

        double mn = e * e * Math.cos(Bm) * Math.cos(Bm);

        double V = Math.sqrt(1 + mn);
        double N = c / V;
        double t =  Math.tan(Bm);

        double r01 = N * Math.cos(Bm);
        double r21 = (N * Math.cos(Bm) * (1 + mn - 9 * mn * t * t)) / (24 * Math.pow(V, 4));
        double r03 = -(N * Math.pow(Math.cos(Bm), 3) * t * t) / 24;

        double s10 = N / (V * V);
        double s12 = -(N * Math.cos(Bm) * Math.cos(Bm) * (2 + 3 * t * t + 3 * t * t * mn)) / (24 * V * V);
        double s30 = (N * (mn - mn * t * t)) / (8 * Math.pow(V, 6));

        double u1 = r01 * deltaL + r21 * deltaL * deltaB * deltaB + r03 * deltaL * deltaL * deltaL;
        double u2 = s10 * deltaB + s12 * deltaB * deltaL * deltaL + s30 * deltaB * deltaB * deltaB;

        result = Math.sqrt(u1 * u1 + u2 * u2);
        return result;
    }

    public static double SurfaceArea(double L1, double B1, double L2, double B2)
    {
        double deltaL = (L2 + L1) / 2;
        double deltaB = B2 - B1;
        double Bm = (B1 + B2) / 2;
        //�����߶�2.984
        double area = 2 * (fb) * (fb) * deltaL * (
                A * Math.sin(0.5 * deltaB) * Math.cos(Bm) -
                        B * Math.sin(1.5 * deltaB) * Math.cos(3 * Bm) +
                        C * Math.sin(2.5 * deltaB) * Math.cos(5 * Bm) -
                        D * Math.sin(3.5 * deltaB) * Math.cos(7 * Bm) +
                        E * Math.sin(4.5 * deltaB) * Math.cos(9 * Bm)
        );

        return area;
    }

    public static double SurfaceAreaEx(double L1, double B1, double L2, double B2, double dB)
    {
        double result = 0;
        int step = 50;//(int)Math.Ceiling(Math.Abs(B2 - B1) / dB);
        if (step > 0)
        {
            double deltaL = (L2 - L1) / step;
            double deltaB = (B2 - B1) / step;

            for (int i = 0; i < step; i++)
            {
                double a =
                        SurfaceArea(L1 + i * deltaL, B1 + i * deltaB, L1 + i * deltaL + deltaL, B1 + i * deltaB + deltaB);
                result += a;
            }
        }
        return result;
    }

    public static double SurfaceAreaEx2(double L1, double B1, double L2, double B2, double dB)
    {
        double ZERO = 0.000000000001;
        int sign;
        if (B1 > B2)
            sign = -1;
        else
            sign = 1;
        double GoodArea = 0;
        if (Math.abs(B2 - B1) < ZERO)
            return ZERO;
        else
        {
            int N = (int)Math.ceil(Math.abs(B2 - B1) / dB); //NΪ��һ��ͼ�߷ֳɶ��ٵȷ�
            double y = (B1 - B2) / N; //yΪγ��ϸ�ֵĵľ���
            double k = (L1 - L2) / (B1 - B2); //Ϊֱ�ߵ�б��
            for (int i = 1; i <= N; i++)
            {
                L1 = L2 + k * y;
                B1 = B2 + y;
                GoodArea = SurfaceArea(L1, B1, L2, B2) + GoodArea;
                L2 = L1;
                B2 = B1;
            }
            return sign * Math.abs(GoodArea);
        }
    }

    public static double Decimal2Rad(double d)
    {
        return d * Math.PI / 180;
    }

    /*
     * ��wkt��ʾ��polygon����multipolygon���������
     */
    public static double EllipsoidArea(String wkt){
        String[] rings = GetRings(wkt);
        if(rings!=null){
            double totalArea = 0.0;
            for(int i=0;i<rings.length;i++){
                double ringArea = 0.0;
                List<Point> pointLst = GetRingPoints(rings[i]);
                for(int j=0;j<pointLst.size()-1;j++){
                    double area = MeasureClass.SurfaceArea(
                            MeasureClass.Decimal2Rad(pointLst.get(j).getX()),
                            MeasureClass.Decimal2Rad(pointLst.get(j).getY()),
                            MeasureClass.Decimal2Rad(pointLst.get(j+1).getX()),
                            MeasureClass.Decimal2Rad(pointLst.get(j+1).getY()));
                    ringArea+=area;
                }
                totalArea+=ringArea;
            }
            return totalArea;
        }
        return 0;
    }

    /*
     * ��wkt��ʾ��polygon����multipolygon���������
     */
    public static double EllipsoidLength(String wkt){
        String[] rings = GetRings(wkt);
        if(rings!=null){
            double totalLength = 0.0;
            for(int i=0;i<rings.length;i++){
                double ringLength = 0.0;
                List<Point> pointLst = GetRingPoints(rings[i]);
                for(int j=0;j<pointLst.size()-1;j++){
                    double length = MeasureClass.CrossMeters(
                            MeasureClass.Decimal2Rad(pointLst.get(j).getX()),
                            MeasureClass.Decimal2Rad(pointLst.get(j).getY()),
                            MeasureClass.Decimal2Rad(pointLst.get(j+1).getX()),
                            MeasureClass.Decimal2Rad(pointLst.get(j+1).getY()));

                    ringLength+=length;
                }
                totalLength+=ringLength;
            }
            return totalLength;
        }
        return 0;
    }

    /*
     * ��ȡring
     */
    public static String[] GetRings(String wkt){
        String[] rings = null;
        if(wkt.startsWith("POLYGON")){
            wkt = wkt.replace("POLYGON (", "");
            wkt = wkt.substring(0, wkt.lastIndexOf(")"));

            String[] paths = wkt.split("\\),");
            rings = new String[paths.length];
            for(int i=0;i<paths.length;i++){
                paths[i] = paths[i].replaceAll("\\(", "").replaceAll("\\)", "");
                rings[i] = paths[i];
            }

            return rings;
        }else if(wkt.startsWith("MULTIPOLYGON")){
            wkt = wkt.replace("MULTIPOLYGON (", "");
            wkt = wkt.substring(0, wkt.lastIndexOf(")"));

            String[] polygons = wkt.split("\\)\\),");
            List<String> ringList = new ArrayList<String>();
            for(int i=0;i<polygons.length;i++){
                String[] paths =  wkt.split("\\),");
                for(int j=0;j<paths.length;j++){
                    paths[j] = paths[j].replaceAll("\\(", "").replaceAll("\\)", "");
                    ringList.add(paths[j]);
                }
            }
            rings = new String[ringList.size()];
            for(int i=0;i<ringList.size();i++){
                rings[i] = ringList.get(i);
            }
        }
        return rings;
    }

    public static List<Point> GetRingPoints(String ring){
        List<Point> pointList = new ArrayList<Point>();
        String[] paths = ring.split("\\),\\(");
        for(String path : paths){
            String points[] = path.split(",");
            //����������ת������Ļ����
            for (String str : points) {
                String xy[] = str.trim().split(" ");
                Point p = new Point(Double.parseDouble(xy[0]), Double.parseDouble(xy[1]));
                pointList.add(p);
            }
        }
        return pointList;
    }

    public static void main(String[] args) throws Exception {
        //System.out.println(EllipsoidArea(multiPolygon));
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "namenode,datanode1,datanode2");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        HTablePool datatablePool=new HTablePool(3, conf, "LCRA1");
        HTablePool indexdatatablePool=new HTablePool(3, conf,  Utils.GetSpatialIndexTableNameByDataTableName("LCRA1"));
        String[] categoryCodeArr=new String[1];
        categoryCodeArr[0] = "0000";
        List<Tuple2<String, String>> indexedData = SpatialQueryCondition.GetOptimalSpatialDataKey(indexdatatablePool, categoryCodeArr, testwkt, "hhh");
        List<Get> getList = new ArrayList<Get>();
        for(int i=10000;i<16277;i++){
            String[] arr = indexedData.get(i)._2.split(Utils.DEFAULT_INPUT_DELIMITER);
            if(Boolean.parseBoolean(arr[1])){
                Get get = new Get(Bytes.toBytes(indexedData.get(i)._1));
                get.addFamily(Utils.COLUMNFAMILY_GEOMETRY);
                get.addFamily(Utils.COLUMNFAMILY_STATINDEX);
                getList.add(get);
            }
        }

        Result[]results = datatablePool.GetTableInstance().get(getList);
        Geometry rightGeo = GeometryEngine.geometryFromWkt(testwkt, 0, Geometry.Type.Polygon);
        int count = 0;
        for(int i=0;i<results.length;i++){
            Result res = results[i];
            if(!res.isEmpty()){
                String rowKey = Bytes.toString(res.getRow());
                String categoryCode = Utils.GetCategoryCode(rowKey);
                String wkt = Bytes.toString(res.getValue(Utils.COLUMNFAMILY_GEOMETRY, Utils.GEOMETRY_QUALIFIER));
                Geometry leftGeo = GeometryEngine.geometryFromWkt(wkt, 0, Geometry.Type.Polygon);

                Geometry geo = GeometryEngine.intersect(rightGeo, leftGeo, null);
                if(!geo.isEmpty()){
                    String wkt1 = GeometryEngine.geometryToWkt(geo, 0);
                    String area =  Bytes.toString(res.getValue(Utils.COLUMNFAMILY_STATINDEX,Bytes.toBytes(StatisticIndexEnum.AREA.toString())));
                    //System.out.println(EllipsoidArea(wkt)+" "+area);
                    System.out.println("sd"+wkt1);
                    System.out.println(EllipsoidArea(wkt1)+" "+area);
                    count++;
                }





                //					try{
//						EllipsoidArea(wkt);
//					}catch(java.lang.NumberFormatException ex){
//						System.out.println(i+" "+wkt);
//					}

            }
            //System.out.println(count);
        }


    }

    public static String testwkt = "POLYGON ((119.93400382995606 30.487993240356445, 119.93396186828613 30.48798942565918, 1"+
            "19.93360710144043 30.487970352172852, 119.93359565734863 30.487970352172852, 119.93334007263184 30.487947463989258, 119.93332481384277 30.4879"+
            "47463989258, 119.93327903747559 30.487943649291992, 119.93324089050293 30.48792839050293, 119.93310356140137 30.487936019897461, 119.932989120"+
            "4834 30.487924575805664, 119.93293952941895 30.4879207611084, 119.93288612365723 30.487897872924805, 119.93283271789551 30.487886428833008, 11"+
            "9.93269920349121 30.487882614135742, 119.9326114654541 30.48786735534668, 119.93255805969238 30.487859725952148, 119.93233680725098 30.4878559"+
            "11254883, 119.93232154846191 30.487855911254883, 119.93212699890137 30.487848281860352, 119.93202781677246 30.487859725952148, 119.93197822570"+
            "801 30.48765754699707, 119.93189811706543 30.487207412719727, 119.93232536315918 30.487237930297852, 119.93260383605957 30.48725700378418, 119"+
            ".93288993835449 30.487272262573242, 119.93334007263184 30.487302780151367, 119.93352699279785 30.48731803894043, 119.93363380432129 30.4873218"+
            "53637695, 119.93393898010254 30.487344741821289, 119.9339771270752 30.48741340637207, 119.93400382995606 30.487577438354492, 119.9336223602294"+
            "9 30.487634658813477, 119.93361854553223 30.487741470336914, 119.93361854553223 30.48777961730957, 119.93366432189941 30.48777961730957, 119.9"+
            "3377113342285 30.487760543823242, 119.93383979797363 30.487756729125977, 119.93400764465332 30.487764358520508, 119.93400382995606 30.48799324"+
            "0356445))";

    public static String multiPolygon ="MULTIPOLYGON (((119.93189811706543 30.487207412719727, 119.93193071130135 30.487209740879436, 119.93189814744602 30.487207583429704, 119.93189811706543 30.487207412719727)), ((119.93281054556853 30.487268028291325, 119.93288993835449 30.487272262573242, 119.93334007263184 30.487302780151367, 119.93336130975156 30.487304513793795, 119.93333869700007 30.487303016000055, 119.93288800900007 30.48727316000003, 119.93281054556853 30.487268028291325)), ((119.93360565081242 30.487320848155235, 119.93363380432129 30.487321853637695, 119.9337671807855 30.487331856872512, 119.93363286800002 30.487322703000075, 119.93360565081242 30.487320848155235)), ((119.9319777758873 30.487655019433085, 119.93198138524302 30.48767042817828, 119.93197822570801 30.48765754699707, 119.9319777758873 30.487655019433085),(119.9319777758873 30.487655019433085, 119.93198138524302 30.48767042817828, 119.93197822570801 30.48765754699707, 119.9319777758873 30.487655019433085)))";

    public static String wkt1 = "POLYGON ((120.03920680200007 30.343228603000064, 120.03869366100004 30.343252263000071, 120.038"+
            "76809700013 30.342926534000068, 120.03883182200003 30.342922461000057, 120.03889208100009 30.342918610000027, 120.03896732600003 30.342922851000029,"+
            "120.03897306400006 30.342869316000083, 120.03898205200004 30.342799159000037, 120.03938678300004 30.342873922000024, 120.03935462000003 30.3431200530"+
            "00067, 120.03919396900005 30.343112834000067, 120.03920680200007 30.343228603000064))";


}
