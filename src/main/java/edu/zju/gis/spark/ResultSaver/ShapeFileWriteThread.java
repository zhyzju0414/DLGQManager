package edu.zju.gis.spark.ResultSaver;

import edu.zju.gis.spark.Common.CommonSettings;
import org.gdal.ogr.*;
import org.gdal.osr.SpatialReference;

import java.util.List;

public class ShapeFileWriteThread implements Runnable {
	public Driver driver;
	public SpatialReference srs;
	public String jobName;
	public List<String> results; 
	public String ds_name;
	public String sindex;
	
	public Layer createPLayer(DataSource pDS, SpatialReference srs, String layerName, int FieldCount){
		Layer pLayer = pDS.CreateLayer(layerName, srs, ogr.wkbMultiPolygon);
		if (pLayer == null)
		{
			System.out.println("Layer Creation Failed!");
			//如果创建失败，删除后重建
			pDS.DeleteLayer(0);
			pLayer = pDS.CreateLayer(layerName, srs, ogr.wkbMultiPolygon);
		}
		for(int i=0;i<FieldCount;i++){
			FieldDefn field = new FieldDefn("FIELD_"+i, ogr.OFTString);
			field.SetWidth(10);
			pLayer.CreateField(field);
		}
		return pLayer;
	}
	
	@Override
	public void run() {
		String ds_full_path = CommonSettings.DEFAULT_LOCAL_OUTPUT_PATH+jobName+"/"+ds_name;
		System.out.println("ds_full_path= "+ds_full_path);
		//create shp
		try{
			DataSource pDS = driver.CreateDataSource(ds_full_path);
			System.out.println("pDS: "+ pDS.GetName());
			if(pDS == null)
			{
				System.out.println("DataSource Creation Error!");
				//如果创建失败，删除后重建
				driver.DeleteDataSource(ds_full_path);
				pDS = driver.CreateDataSource(ds_full_path);
			}
			int FeildCount=0;
			if(results.size()>0){
				System.out.println("result size = "+results.size());
				FeildCount=results.get(0).split(CommonSettings.DEFAULT_OUTPUT_DELIMITER).length;
				System.out.println("fieldCount = "+FeildCount);
			}
			
			Layer pLayer = createPLayer(pDS, srs, ds_name,FeildCount-1);
			int spatialindex=Integer.valueOf(FeildCount-1);
			System.out.println("spatialindex"+spatialindex);
			int count =0;
			for(String str : results){
				count++;
				System.out.println("count="+count);
				String[] fields = str.split(CommonSettings.DEFAULT_OUTPUT_DELIMITER);
				Feature pfeature = new Feature(pLayer.GetLayerDefn());
				for(int i = 0;i<fields.length;i++){
					if(i<spatialindex){
						pfeature.SetField(i, fields[i]);
					}else if(i == spatialindex){
						pfeature.SetGeometry(org.gdal.ogr.Geometry.CreateFromWkt(fields[i]));
//						System.out.println("字段："+fields[i]);
					}else if(i > spatialindex){
						pfeature.SetField(i-1, fields[i]);
					}
				}
//				String str2 = "REPACK " + pLayer.GetName();
//				pDS.ExecuteSQL(str2, null, "");
				pLayer.CreateFeature(pfeature);
				pfeature.delete();
			}
//			pLayer.SyncToDisk();
			pLayer.delete();
			pDS.delete();
		}catch(Exception e){
			System.out.println(e.getMessage());
			System.out.println("SHP:"+ds_name+" create failed!");
			//如果异常，删除图层
			driver.DeleteDataSource(ds_full_path);
		}
	}

}
