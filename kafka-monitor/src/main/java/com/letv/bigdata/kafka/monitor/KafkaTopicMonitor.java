package com.letv.bigdata.kafka.monitor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.letv.bigdata.util.PropertiesUnit;

public class KafkaTopicMonitor {
	public static PropertiesUnit conf  = new PropertiesUnit("monitor.properties");
	private static final Logger log = LoggerFactory.getLogger(KafkaTopicMonitor.class);
	private SimpleConsumer kafka_consumer = null;
	private String client_id = "kafkaTopicMonitor_client";
	private String kafkaBrokerHost = "";
	private String clusterName = "";
	
	public KafkaTopicMonitor(String c_kafkaBrokerHost,String c_cluseterName){
		log.info("-------------init KafkaTopicMonitor----------------");
		this.clusterName = c_cluseterName;
		String hosts[] = c_kafkaBrokerHost.split(",");
		for(String host : hosts){
			try {
				kafka_consumer = new SimpleConsumer(host, 9092, 6000,
						1024 * 64, this.client_id);
				this.kafkaBrokerHost = host;
				List<String> topics = Arrays.asList(conf.getValue("useraction.kafka.monitor.topics"));
				getTopicMeta(topics);
				log.info(clusterName+ " monitor on server: "+kafkaBrokerHost);
				break;
			} catch (Exception e) {
				
			}
		}
	}
	
	/**
	 * 获取kafka的topic的元数据信息。
	 * @param topics
	 * @return
	 */
	public List<PartitionMetadata> getTopicMeta(List<String> topics){
		List<PartitionMetadata> pmList = new ArrayList<PartitionMetadata>();
		TopicMetadataRequest request = new TopicMetadataRequest(topics);
		TopicMetadataResponse response = kafka_consumer.send(request);
		if (response != null && response.topicsMetadata() != null) {
			for (TopicMetadata tm : response.topicsMetadata()) {
				for (PartitionMetadata pm : tm.partitionsMetadata()) {
					pmList.add(pm);
					System.out.println(pm.leader().host() + " " + tm.topic()+ " " + pm.partitionId());
				}
			}
		}
		return pmList;
	}
	/**
	 * 获取kafka的topic的最新的offset信息：每天的固定时间读取，并保存下来，可以分析kafka topic 的数据量增长情况。
	 * @param topics
	 * @throws IOException
	 */
	public void monitorTopicOffset(String topics) throws IOException{
		List<String> topicList = Arrays.asList(topics.split(","));
		TopicMetadataRequest request = new TopicMetadataRequest(topicList);
		TopicMetadataResponse response = kafka_consumer.send(request);
		if (response != null && response.topicsMetadata() != null) {
			for (TopicMetadata tm : response.topicsMetadata()) {
				for (PartitionMetadata pm : tm.partitionsMetadata()) {
					OffsetMonitor om = new OffsetMonitor( tm.topic(), pm.partitionId(), pm.leader().host());
					String  topicname = tm.topic();           //2
					Integer  partitionid = pm.partitionId();    //3
					String  leader = pm.leader().host();          //4
					Long start_offset = om.getEarliestOffset();       //5
					
					Long current_offset = om.getLatestOffset();         //6
					
			       saveStatusToDatabase(clusterName, topicname,partitionid.toString(),leader,start_offset.toString(), current_offset.toString());
			        
//					System.out.println(tm.topic()  + "\t" + pm.partitionId() + "\t" + pm.leader().host()+"\t" + om.getEarliestOffset() + "\t" + om.getLatestOffset());
				}
			}
		}
	}
	/**
	 * 按日期监控kafka的topic的offset信息：每天的固定时间读取一次topic的offset信息。
	 * @param topics
	 * @param time_str
	 * @throws Exception
	 */
	public void monitorTopicOffsetByDay(String topics,String time_str) throws Exception{
		List<String> topicList = Arrays.asList(topics.split(","));
		TopicMetadataRequest request = new TopicMetadataRequest(topicList);
		TopicMetadataResponse response = kafka_consumer.send(request);
		if (response != null && response.topicsMetadata() != null) {
			for (TopicMetadata tm : response.topicsMetadata()) {
				for (PartitionMetadata pm : tm.partitionsMetadata()) {
//					System.out.println(pm.leader().host() + " " + tm.topic()+ " " + pm.partitionId());
					OffsetMonitor om = new OffsetMonitor(tm.topic(),pm.partitionId(),pm.leader().host());
					Long offset = om.getOffsetByTimeString(time_str);
					System.out.println(tm.topic()  + "\t" + pm.partitionId() + "\t" + pm.leader().host()+"\t" + time_str+ "\t" + offset);
				}
			}
		}
	}
	
	public void monitorIsrAndReplica(String topics){
		List<String> topicList = Arrays.asList(topics.split(","));
		TopicMetadataRequest request = new TopicMetadataRequest(topicList);
		TopicMetadataResponse response = kafka_consumer.send(request);
		if (response != null && response.topicsMetadata() != null) {
			for (TopicMetadata tm : response.topicsMetadata()){
				for (PartitionMetadata pm : tm.partitionsMetadata()){
					List<Broker> isr = pm.isr();
					StringBuilder sb = new StringBuilder();
					sb.append("isr(");
					for(Broker broker :isr){
						sb.append(broker.id()).append(":").append(broker.host()).append(";");
					}
					sb.append(")");
					
					StringBuilder sb_replicas = new StringBuilder();
					List<Broker> replicas = pm.replicas();
					sb_replicas.append("replicas(");
					for(Broker broker :replicas){
						sb_replicas.append(broker.id()).append(":").append(broker.host()).append(";");
					}
					sb_replicas.append(")");
					String  topicname = tm.topic();              //2
					Integer  partitionid = pm.partitionId();    //3
					String  leader = pm.leader().host();          //4
					String isr1 = sb.toString();       //5
					String replicas1 = sb_replicas.toString();         //6
					
			       saveStatusToDatabase01(clusterName, topicname,partitionid.toString(),leader,isr1.toString(), replicas1.toString());
//				   System.out.println(tm.topic() + "\t" + pm.partitionId()+ "\t" + pm.leader().host() + "\t" + sb.toString() + "\t" + replicas1);
				}
			}
		}
	}
	
	
	private void saveStatusToDatabase01(String clustername, String topicname,
			String partitionid, String leader, String isr, String replicas) {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
        	//加载Connector/J驱动  
        	//也可写为：Class.forName("com.mysql.jdbc.Driver"); 
			String drivername = conf.getValue("db.drivername");
            Class.forName(drivername);
            //配置信息
            String dbUrl = conf.getValue("db.url");
            String dbUserName = conf.getValue("db.username");
            String dbPassword = conf.getValue("db.password");
    		
            //建立到MySQL的连接  
			conn = DriverManager.getConnection(dbUrl, dbUserName,
                    dbPassword);
			
			//执行sql语句
            ps = conn.prepareStatement("insert into tb_kafka_topic_isr (dt,clustername,topicname,partitionid,leader,isr,replicas) values(?,?,?,?,?,?,?);");
            Date nowTime = new Date(System.currentTimeMillis());
            SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
            String retStrFormatNowDate = sdFormatter.format(nowTime);
            ps.setString(1,retStrFormatNowDate);
            ps.setString(2, clustername);
            ps.setString(3, topicname);
            ps.setString(4, partitionid);
            ps.setString(5, leader);
            ps.setString(6, isr);
            ps.setString(7, replicas);
            ps.execute();
            ps.close();
            conn.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
        	if(ps != null) {
        		try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
        	}
        	if(conn!= null){
        		try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
        	}
        }
		
	}

	private static void saveStatusToDatabase(String clustername,String topicname,String partitionid ,String leader,String start_offset,String current_offset) 
	{
		Connection conn = null;
		PreparedStatement ps = null;
        try {
        	//加载Connector/J驱动  
        	//这一句也可写为：Class.forName("com.mysql.jdbc.Driver"); 
			String drivername = conf.getValue("db.drivername");
            Class.forName(drivername);
            //配置信息
            String dbUrl = conf.getValue("db.url");
            String dbUserName = conf.getValue("db.username");
            String dbPassword = conf.getValue("db.password");
    		
            //建立到MySQL的连接  
			conn = DriverManager.getConnection(dbUrl, dbUserName,
                    dbPassword);
			
			//执行sql语句
            ps = conn.prepareStatement("insert into tb_kafka_topic_offset (dt,clustername,topicname,partitionid,leader,start_offset,current_offset) values(?,?,?,?,?,?,?);");
            SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
            String retStrFormatNowDate = sdFormatter.format(new Date());
            ps.setString(1,retStrFormatNowDate);
            ps.setString(2, clustername);
            ps.setString(3, topicname);
            ps.setString(4, partitionid);
            ps.setString(5, leader);
            ps.setString(6, start_offset);
            ps.setString(7, current_offset);
            ps.execute();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
        	if(ps != null) {
        		try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
        	}
        	if(conn!= null){
        		try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
        	}
        }

    }

	public static void main(String[] args) throws Exception {
		String topics = KafkaTopicMonitor.conf.getValue("useraction.kafka.monitor.topics");
		String user_action_broker_host = KafkaTopicMonitor.conf.getValue("useraction.kafka.monitor.brokerlist");
		KafkaTopicMonitor user_action= new KafkaTopicMonitor(user_action_broker_host,"kafka-user-action");
		System.out.println("--------------monitorTopicOffset--------------------");
		user_action.monitorTopicOffset(topics);
		System.out.println("----------------ok------------------");
	}
}
