package com.db.picture;

import java.awt.Container;
import java.awt.Font;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.time.Month;
import org.jfree.data.time.Hour;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;

public class TimeSeriesChart {
	public static SimpleDateFormat sdf = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	ChartPanel frame1;

	public TimeSeriesChart() throws Exception {

		XYDataset xydataset = createDataset();
		JFreeChart jfreechart = ChartFactory.createTimeSeriesChart(
				"kafkatopic", "日期", "流量", xydataset, true, true, true);
		XYPlot xyplot = (XYPlot) jfreechart.getPlot();
		DateAxis dateaxis = (DateAxis) xyplot.getDomainAxis();
		dateaxis.setDateFormatOverride(new SimpleDateFormat("dd-MMM-yyyy"));
		frame1 = new ChartPanel(jfreechart, true);
		dateaxis.setLabelFont(new Font("黑体", Font.BOLD, 14)); // 水平底部标题
		dateaxis.setTickLabelFont(new Font("宋体", Font.BOLD, 12)); // 垂直标题
		ValueAxis rangeAxis = xyplot.getRangeAxis();// 获取柱状
		rangeAxis.setLabelFont(new Font("黑体", Font.BOLD, 15));
		jfreechart.getLegend().setItemFont(new Font("黑体", Font.BOLD, 15));
		jfreechart.getTitle().setFont(new Font("宋体", Font.BOLD, 20));// 设置标题字体

	}

	private static XYDataset createDataset() throws Exception { 
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("找不到驱动类");
			e.printStackTrace();
		}

		String url = "jdbc:mysql://10.180.92.243:3844/cluster_monitor";
		String user = "cluster_monitor_w";
		String password = "ZThmNzg3ZWYxNDJ";
		Connection conn = DriverManager.getConnection(url, user, password);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt
				.executeQuery("select dt,sum(current_offset) from tb_kafka_topic_offset GROUP BY dt");
		ResultSetMetaData data = rs.getMetaData();
		ArrayList<HashMap<String, String>> al = new ArrayList<HashMap<String, String>>();
		TimeSeries timeseries = new TimeSeries("kafkatopic",
				org.jfree.data.time.Hour.class);
		rs.next();
		String fisrtdt=rs.getString(1);
		Long firstValue = rs.getLong(2);	
		while (rs.next()) {
			HashMap<String, Long> map = new HashMap<String, Long>();
			String dt = rs.getString(1);
			Long value = rs.getLong(2);
			timeseries.addOrUpdate(new Hour(sdf.parse(dt)), value-firstValue);
			firstValue = value;
		}

		TimeSeriesCollection timeseriescollection = new TimeSeriesCollection();
		timeseriescollection.addSeries(timeseries);

		System.out.println(al);
		rs.close();
		stmt.close();
		conn.close();
		return timeseriescollection;
	}
	public void getData(){
		
	}
	public ChartPanel getChartPanel() {
		return frame1;

	}
}
