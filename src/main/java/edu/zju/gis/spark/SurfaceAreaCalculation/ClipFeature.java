package edu.zju.gis.spark.SurfaceAreaCalculation;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.WktImportFlags;

import java.io.Serializable;

/**
 * Created by dlgq on 2017/8/7.
 */
public class ClipFeature implements Serializable {
    private double env_xmin;
    private double env_xmax;
    private double env_ymin;
    private double env_ymax;
    private String featureID;
    private String geo;
    private Geometry Geo;
    private String TFcode;   //这个要素所属于的图幅号  10m分辨率和2m分辨率分别对应了两种图幅号
    private void setGeo(org.gdal.ogr.Geometry geometry){ this.Geo = GeometryEngine.geometryFromWkt(geometry.ExportToWkt(), WktImportFlags.wktImportDefaults, Geometry.Type.Unknown); }
    public Geometry getGeo(){ return Geo;}
    public void setTFcode(String tFcode)
    {
        this.TFcode = tFcode;
    }
    public String getTFcode()
    {
        return TFcode;
    }

    public void setFeatureID(String featureID){this.featureID = featureID;}
    public String getFeatureID(){return featureID;}
    public void setEnv_xmin(double env_xmin)
    {
        this.env_xmin = env_xmin;
    }
    public void setEnv_xmax(double env_xmax)
    {
        this.env_xmax = env_xmax;
    }
    public void setEnv_ymin(double env_ymin)
    {
        this.env_ymin = env_ymin;
    }
    public void setEnv_ymax(double env_ymax)
    {
        this.env_ymax = env_ymax;
    }
    public double getEnv_xmin()
    {
        return env_xmin;
    }
    public double getEnv_xmax()
    {
        return env_xmax;
    }
    public double getEnv_ymin()
    {
        return env_ymin;
    }
    public double getEnv_ymax()
    {
        return env_ymax;
    }
    public ClipFeature(org.gdal.ogr.Geometry geo,String featureID){
        double[] env =new double[4];
        geo.GetEnvelope(env);
        setGeo(geo);
        setFeatureID(featureID);
        setEnv_xmin(env[0]);
        setEnv_xmax(env[1]);
        setEnv_ymin(env[2]);
        setEnv_ymax(env[3]);
    }
}
