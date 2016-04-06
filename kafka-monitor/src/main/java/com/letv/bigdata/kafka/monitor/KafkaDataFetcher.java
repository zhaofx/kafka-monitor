package com.letv.bigdata.kafka.monitor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.letv.bigdata.util.FileWriterTool;
/**
 * kafka的的消息读取工具类。
 * @author Administrator
 *
 */
public class KafkaDataFetcher {
	private static final Logger log = LoggerFactory.getLogger(KafkaDataFetcher.class);
	private SimpleConsumer kafka_consumer = null;
	private String client_id = "kafka_data_fetcher_client";
	private String topic;
	private Integer partition;
	private String leader;
	public KafkaDataFetcher(String c_topic ,Integer c_partition, String c_leader){
		this.topic = c_topic;
		this.partition = c_partition;
		this.leader = c_leader;
		this.kafka_consumer = new SimpleConsumer(leader, 9092, 6000, 1024*64, this.client_id);
	}
	/**
	 * 从startOffset拉取2M的kafka的消息。
	 * @param startOffset
	 * @return
	 * @throws IOException
	 */
	public ByteBufferMessageSet feachData(Long startOffset) throws IOException {
		FetchRequestBuilder requestBuilder = new FetchRequestBuilder();
		FetchRequest req = requestBuilder.clientId(this.client_id)
										 .addFetch(this.topic, this.partition, startOffset, 2 * 1024 * 1024)
										 .build();
		FetchResponse fetchResponse = kafka_consumer.fetch(req);
		if (fetchResponse.hasError()) {
			short code = fetchResponse.errorCode(topic, partition);
			log.error("getEndOffset Error ：fetching data from the Broker:" + partition + " Reason: " + code);
			if (this.kafka_consumer != null) {
				this.kafka_consumer.close();
				this.kafka_consumer = null;
			}
			throw new IOException("getEndOffset Error ：fetching data from the Broker:" + this.partition + " Reason: " + code);
		}
		return fetchResponse.messageSet(topic, partition);
	}
	/**
	 * 读取startOffset 至endOffset的kafka消息，并打印到output文件中。
	 * @param startOffset
	 * @param endOffset
	 * @param output
	 * @throws IOException
	 */
	public void fetchData(Long startOffset,Long endOffset,String output) throws IOException{
		Long currentOffset = startOffset;
		FileWriterTool writer = new FileWriterTool(output);
		String msg = "";
		MessageAndOffset messageAndOffset = null;
		while (currentOffset < endOffset-1) {
			ByteBufferMessageSet msgset = feachData(currentOffset);
			Iterator<MessageAndOffset> it = msgset.iterator();
			while (it.hasNext()) {
				messageAndOffset = it.next();
				currentOffset = messageAndOffset.offset();
				if (currentOffset > endOffset) {
					break;
				}
				ByteBuffer payload = messageAndOffset.message().payload();
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				msg = new String(bytes);
				writer.writeStrings(msg + "\n");
			}
			System.out.println(currentOffset);
		}
		writer.close();
	}
	/**
	 * 从startOffset拉取2M的kafka的消息并打印出来。
	 * @param startOffset
	 * @return
	 * @throws IOException
	 */
	public void feachDataPrint(Long startOffset) throws IOException{
		FetchRequestBuilder requestBuilder = new FetchRequestBuilder();
		FetchRequest req = requestBuilder.clientId(this.client_id)
										 .addFetch(this.topic, this.partition, startOffset, 2 * 1024 * 1024)
										 .build();
		FetchResponse fetchResponse = kafka_consumer.fetch(req);
		if (fetchResponse.hasError()) {
			short code = fetchResponse.errorCode(topic, partition);
			log.error("getEndOffset Error ：fetching data from the Broker:" + partition + " Reason: " + code);
			if (this.kafka_consumer != null) {
				this.kafka_consumer.close();
				this.kafka_consumer = null;
			}
			throw new IOException("getEndOffset Error ：fetching data from the Broker:" + this.partition + " Reason: " + code);
		}
		MessageAndOffset messageAndOffset = null;
		Iterator<MessageAndOffset> it = fetchResponse.messageSet(topic, partition).iterator();
		String msg = "";
		while (it.hasNext()){
			messageAndOffset =   it.next();
			ByteBuffer payload = messageAndOffset.message().payload();
			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);
			msg = new String(bytes);
			log.info(messageAndOffset.offset() + " : " +msg);
		}
	}
}
