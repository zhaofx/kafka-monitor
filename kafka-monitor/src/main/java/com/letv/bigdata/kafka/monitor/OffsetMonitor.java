package com.letv.bigdata.kafka.monitor;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.letv.cluster.monitor.Connection;
/**
 * kafka的offset监控程序
 * @author zfx
 *
 */
public class OffsetMonitor {
	private static final Logger log = LoggerFactory.getLogger(OffsetMonitor.class);
	private SimpleConsumer kafka_consumer = null;
	private SimpleDateFormat date_format = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
	private String client_id = "offsetMonitor_client";
	private String topic;
	private Integer partition;
	private String leader;
	public Long getOffset(String topic){
		Long offset = 0L;
		return offset;
	}
	public OffsetMonitor(String c_topic ,Integer c_partition, String c_leader){
		this.topic = c_topic;
		this.partition = c_partition;
		this.leader = c_leader;
		this.kafka_consumer = new SimpleConsumer(leader, 9092, 6000, 1024*64, this.client_id);
	}
	 
	/*
	 * 得到partition的offset Finding Starting Offset for Reads
	 */
	public Long getOffset(Long time) throws IOException {
		TopicAndPartition topicAndPartition = new TopicAndPartition(this.topic, this.partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(time, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
				requestInfo, kafka.api.OffsetRequest.CurrentVersion(), this.client_id);
		OffsetResponse response = this.kafka_consumer.getOffsetsBefore(request);
		if (response.hasError()) {
			log.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(this.topic, this.partition));
			throw new IOException ("Error fetching kafka Offset by time:" + response.errorCode(this.topic, this.partition));
		}
		return response.offsets(this.topic, this.partition)[0];
	}
	public void getTopicMeta(List<String> topics){
		TopicMetadataRequest request = new TopicMetadataRequest(topics);
		TopicMetadataResponse response = kafka_consumer.send(request);
		if (response != null && response.topicsMetadata() != null) {
			for (TopicMetadata tm : response.topicsMetadata()) {
				for (PartitionMetadata pm : tm.partitionsMetadata()) {
					System.out.println(pm.leader().host() + " " + tm.topic()+ " " + pm.partitionId());
				}
			}
		}
	}
	
	/**
	 * 根据时间字符串查询offset
	 * @param timeStr  格式：yyyyMMdd-HH:mm:ss
	 * @return
	 * @throws Exception
	 */
	public Long getOffsetByTimeString(String timeStr) throws Exception{
		Long times = date_format.parse(timeStr).getTime();
		return getOffset(times);
	}
	/**
	 * 获取最早的offset
	 * @return
	 * @throws IOException
	 */
	public Long getEarliestOffset() throws IOException{
		return getOffset(OffsetRequest.EarliestTime());
	}
	/**
	 * 获取最晚的offset
	 * @return
	 * @throws IOException
	 */
	public Long getLatestOffset() throws IOException{
		return getOffset(OffsetRequest.LatestTime());
	}
	
	public static void main(String[] args) throws IOException {
//		List<String> topics = Arrays.asList(conf.get("kafka.topics").split(","));
		OffsetMonitor monitor = new OffsetMonitor("app_request",0,"10-149-11-234");
		System.out.println(monitor.getEarliestOffset());
		System.out.println(monitor.getLatestOffset());
		
		monitor = new OffsetMonitor("app_request",1,"10-149-11-235");
		System.out.println(monitor.getEarliestOffset());
		System.out.println(monitor.getLatestOffset());
		
		System.out.println("-----------------------------");
		monitor = new OffsetMonitor("xxx",0,"10-149-10-41");
		List <String> topics = new ArrayList<String>();
		topics.add("app_request");
		topics.add("environment_request");
		topics.add("event_request");
		topics.add("heartbeat_request");
		topics.add("musicPlay_request");
		topics.add("play_request");
		topics.add("widget_request");
		monitor.getTopicMeta(topics);
	}
}
