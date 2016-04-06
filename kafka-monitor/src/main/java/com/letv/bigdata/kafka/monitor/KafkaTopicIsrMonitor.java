package com.letv.bigdata.kafka.monitor;
/**
 * 监控kafka的topic的isr列表信息
 * @author zfx
 *
 */
public class KafkaTopicIsrMonitor {
	public static void main(String[] args) {
		String topics = KafkaTopicMonitor.conf.getValue("useraction.kafka.monitor.topics");
		String user_action_broker_host = KafkaTopicMonitor.conf.getValue("useraction.kafka.monitor.brokerlist");
		KafkaTopicMonitor user_action= new KafkaTopicMonitor(user_action_broker_host,"kafka-user-action");
		System.out.println("----------------monitorIsrAndReplica------------------");
		user_action.monitorIsrAndReplica(topics);
		System.out.println("----------------ok------------------");
	}
}
