/*
 Copyright 1995-2013 Esri

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 For additional information, contact:
 Environmental Systems Research Institute, Inc.
 Attn: Contracts Dept
 380 New York Street
 Redlands, California, USA 92373

 email: contracts@esri.com
 */


package com.esri.core.geometry;

import java.io.Serializable;

import scala.reflect.internal.Trees.This;

/**
 * A polygon is a collection of one or many interior or exterior rings.
 */
public final class Polygon extends MultiPath implements Serializable {

	private static final long serialVersionUID = 2L;// TODO:remove as we use
													// writeReplace and
													// GeometrySerializer

	/**
	 * Creates a polygon.
	 */
	public Polygon() {
		m_impl = new MultiPathImpl(true);
	}

	Polygon(VertexDescription vd) {
		m_impl = new MultiPathImpl(true, vd);
	}

	@Override
	public Geometry createInstance() {
		return new Polygon(getDescription());
	}

	@Override
	public int getDimension() {
		return 2;
	}

	@Override
	public Geometry.Type getType() {
		return Type.Polygon;
	}

	/**
	 * Calculates the ring area for this ring.
	 * 
	 * @param ringIndex
	 *            The index of this ring.
	 * @return The ring area for this ring.
	 */
	public double calculateRingArea2D(int ringIndex) {
		return m_impl.calculateRingArea2D(ringIndex);
	}

	/**
	 * Returns TRUE if the ring is an exterior ring. Valid only for simple
	 * polygons.
	 */
	public boolean isExteriorRing(int partIndex) {
		return m_impl.isExteriorRing(partIndex);
	}

	/**
	 * Returns TRUE when this geometry has exactly same type, properties, and
	 * coordinates as the other geometry.
	 */
	@Override
	public boolean equals(Object other) {
		if (other == null)
			return false;

		if (other == this)
			return true;

		if (other.getClass() != getClass())
			return false;

		return m_impl.equals(((Polygon) other)._getImpl());
	}

	/**
	 * Returns a hash code value for this polygon.
	 */

	@Override
	public int hashCode() {
		return m_impl.hashCode();
	}

	/**
	 * Sets a new vertex for the polygon.
	 * 
	 * @param i
	 *            The index of the new vertex.
	 * @param x
	 *            The X coordinate for the new vertex.
	 * @param y
	 *            The Y coordinate for the new vertex.
	 */
	public void setXY(int i, double x, double y) {
		m_impl.setXY(i, x, y);

	}

	void interpolateAttributes(int path_index, int from_point_index,
			int to_point_index) {
		m_impl.interpolateAttributes(path_index, from_point_index,
				to_point_index);
	}

	void interpolateAttributes(int semantics, int path_index,
			int from_point_index, int to_point_index) {
		m_impl.interpolateAttributesForSemantics(semantics, path_index,
				from_point_index, to_point_index);
	}

	public int getExteriorRingCount() {
		return m_impl.getOGCPolygonCount();
	}
	
	public Point GetInteriorPoint(){
	
			Envelope mbr = new Envelope();
			this.queryEnvelope(mbr);
			Line line = new Line();
			boolean isYLength = false;
			if(mbr.getXMax()-mbr.getXMin()>mbr.getYMax()-mbr.getYMin()){
				line.setStart(new Point(mbr.getCenterX(),mbr.getYMin()));
				line.setEnd(new Point(mbr.getCenterX(),mbr.getYMax()));
				isYLength = false;
			}else{
				line.setStart(new Point(mbr.getXMin(),mbr.getCenterY()));
				line.setEnd(new Point(mbr.getXMax(),mbr.getCenterY()));
				isYLength = true;
			}
			Polyline polyline = (Polyline) GeometryEngine.intersect(line, this, null);
			   Point[] points = new Point[2];
			   points[0] = mbr.getCenter();
			   points[1] = mbr.getCenter();
			   double maxlength = 0;
			   for(int i=0;i<polyline.getPointCount()-1;i+=2){
				   double length = calculateDistance(polyline.getPoint(i),polyline.getPoint(i+1));
				   if(length>maxlength){
					   points[0] = polyline.getPoint(i);
					   points[1] = polyline.getPoint(i+1);
					   length = maxlength;
				   }
			   }
			if(isYLength){
				return new Point((points[0].getX()+points[1].getX())/2,mbr.getCenterY());
			}else{
				return new Point(mbr.getCenterX(),(points[0].getY()+points[1].getY())/2);
			}
	}
	
	public static double calculateDistance(Point p1,Point p2){
		return Math.sqrt((p1.getX()-p2.getX())*(p1.getX()-p2.getX())+(p1.getY()-p2.getY())*(p1.getY()-p2.getY()));
	}

}
