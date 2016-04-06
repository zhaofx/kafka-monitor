package com.letv.bigdata.kafka_monitor;

import java.io.IOException;

import com.letv.bigdata.kafka.monitor.KafkaDataFetcher;
import com.letv.bigdata.kafka.monitor.OffsetMonitor;

public class Test {
	public void testGetOffsetByTimeStr() throws Exception{
		OffsetMonitor monitor = new OffsetMonitor("environment_request",0,"10-149-10-30");
		Long offset = monitor.getOffsetByTimeString("20160107-20:00:00");
		System.out.println("---offset---:"+ offset);
	}
	/**
	 * 获取100次topic的offset，探知kafka的实时数据接收速率。
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void getOffsetIncrise() throws IOException, InterruptedException{
		OffsetMonitor monitor_event_request = new OffsetMonitor("cdn_gzip_data",0,"10-149-11-86");
		for(int i = 0;i<100;i ++){
			System.out.println(monitor_event_request.getEarliestOffset() + "  " + monitor_event_request.getLatestOffset());
			Thread.sleep(5000);
		}
		System.out.println(monitor_event_request.getEarliestOffset() + "  " + monitor_event_request.getLatestOffset());
	}
	/**
	 * 根据offset读取kafka的消息。
	 * @throws Exception
	 */
	public void testGetKafkaMsgByOffset() throws Exception{
		KafkaDataFetcher fetcher = new KafkaDataFetcher("heartbeat_request",0,"10-149-10-41");
		fetcher.feachData(464146962L);
	}

	/**
	 * 将kafka消息保存到本地。
	 * @throws IOException
	 */
	public void testLoadKafkaDataToLocal() throws IOException{
		KafkaDataFetcher fetcher = new KafkaDataFetcher("environment_request",0,"10-149-10-30");
		Long startOffset = 85729384L;
		Long endOffset =   87012798L;
		endOffset =   85749384L;
//		String output = "/home/data_quality/temp/loadKafka/environment_request_85729384_87012798";
		String output = "D:/environment_request_85729384_87012798";
		fetcher.fetchData(startOffset, endOffset, output);
	}
	
	public static void main(String[] args) throws Exception {
		Test test = new Test();
//		test.testGetOffsetByTimeStr();
//		test.testGetKafkaMsgByOffset();
//		test.testLoadKafkaDataToLocal();
		test.getOffsetIncrise();
		System.out.println("------------ok--------------");
	}
}
