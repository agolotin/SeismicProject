package main.java.consumer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import main.java.general.timeseries.SegmentCustom;
import main.java.general.timeseries.TimeseriesCustom;
import main.java.streaming.ignite.server.IgniteCacheConfig;
import main.java.streaming.ignite.server.MeasurementInfo;

/*
 * This class merges Kafka functionality with Ignite client functionality.
 * ConsumerKafka receives timeseries data from the ProducerKafka in the form of
 * serialized TimeseriesCustom objects. These objects are deserialized and 
 * segments are sent to the cache for processing. 
 */
@SuppressWarnings({ "unchecked", "rawtypes", "serial" })
public class ConsumerKafka implements Runnable, Serializable {

	private final KafkaConsumer consumer;
	private final String topic;
	private final int tid;

	private IgniteDataStreamer<String, MeasurementInfo> dataStreamer;
	private IgniteCache<String, MeasurementInfo> streamCache;

	public ConsumerKafka(int tid, String group_id, String topic) {
		this.tid = tid;
		this.topic = topic;

		// Set up the consumer
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", group_id);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "main.java.general.serialization.TimeseriesDecoder");
		props.put("value.deserializer", "main.java.general.serialization.TimeseriesDecoder");

		consumer = new KafkaConsumer<>(props);
	}

	// DOCME: Add documentation to this method, it's really long..
	@Override
	public void run() {
		// log4j writes to stdout for now
		// org.apache.log4j.BasicConfigurator.configure();

		try {			
			this.setup();

			// TODO: This will have to be a command line parameter...probably
			Integer secPerWindow = 5;
			float sampleRate = 20; // default sample rate

			Integer windowNum = 0, i = 0; // i will always be unique
			while (true) {
				ConsumerRecords<String, TimeseriesCustom> records = consumer.poll(Long.MAX_VALUE);
				this.sendData(records, secPerWindow, sampleRate, windowNum, i);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			consumer.close();
		}
	}

	public void shutdown() {
		consumer.wakeup();
	}

	// sets up the environment to send data to Ignite server and
	// cache the data received from the ProducerKafka.
	// Sets up a topic partition, an Ignite configuration for the cache,
	// starts Ignite client, and opens a connection to the Ignite Server.
	private void setup() {
		TopicPartition par = new TopicPartition(topic, tid);

		// Have consumer listen on a specific topic partition
		consumer.assign(Arrays.asList(par));
		consumer.seekToEnd(par);

		IgniteConfiguration conf = new IgniteConfiguration();
		// Since multiple consumers will be running on a single node,
		// we need to specify different names for them
		conf.setGridName(String.valueOf("Grid" + tid));

		// REVIEWME: Review what communication spi does...
		// TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
		// commSpi.setLocalAddress("localhost");
		// commSpi.setLocalPortRange(100);

		// conf.setCommunicationSpi(commSpi);

		conf.setClientMode(true);

		Ignite ignite = Ignition.start(conf);
		this.streamCache = ignite.getOrCreateCache(IgniteCacheConfig.timeseriesCache());

		this.dataStreamer = ignite.dataStreamer(streamCache.getName());

		// For some reason we have to overwrite the value of
		// what's being put into cache...otherwise it doesn't work
		// TESTME: Try get rid of these next 15 or so lines and test Ignite
		// query
		dataStreamer.allowOverwrite(true);

		dataStreamer.receiver(new StreamTransformer<String, MeasurementInfo>() {

			@Override
			public Object process(MutableEntry<String, MeasurementInfo> e, Object... arg)
					throws EntryProcessorException {

				e.setValue((MeasurementInfo) arg[0]);

				return null;
			}
		});

	}

	// Sends data to the Ignite server for caching.
	// Loops through list of records and for each record
	// breaks the data up into measurements, then sends the
	// measurements to the Ignite cache
	private void sendData(ConsumerRecords<String, TimeseriesCustom> records, Integer secPerWindow, float sampleRate,
			Integer windowNum, int i) {

		for (ConsumerRecord record : records) {
			System.out.printf("Record topic = %s, partition number = %d, " + "tid = %d\n", record.topic(),
					record.partition(), tid);

			windowNum = 0;
			i = 0; // override the window number each time new consumer record
					// comes in

			TimeseriesCustom data = (TimeseriesCustom) record.value();
			SegmentCustom segment = data.getSegment();

			// Overwrite the sample rate to be sure
			sampleRate = segment.getSampleRate();

			// FIXME: Figure out the correct statement for this...
			SqlFieldsQuery qry = new SqlFieldsQuery(
					"select _key, _val from measurementinfo where "
					+ "measurementinfo.windownum = ? and measurementinfo.tid = ?");
			// Keep on spinning until we get don't have anything in cache associated with that window anymore
			System.out.println(streamCache.query(qry.setArgs(windowNum, tid)).getAll());
			System.out.printf("tid = %d, cache size = %d, window number = %d\n", tid, streamCache.size(CachePeekMode.ALL), windowNum);			
			System.out.println(streamCache.get(String.valueOf(tid + "_" + i)));

			// TODO: Artem, look at this modification. Is this what you meant
			// for blocking on the cache?
			// while (!streamCache.query(qry.setArgs(windowNum, tid))
			// .getAll().isEmpty()) {
			// System.out.printf("tid = %d, there is data in Ignite cache
			// associated with window number = %d\n", tid, windowNum);
			// Thread.sleep(5000);
			// }

			// Block if the Ignite cache already has a window with that number
			// in it,
			// until that previous window has been processed and is no longer in
			// the way
			while (streamCache.get(String.valueOf(tid + "_" + i)) != null) {
				System.out.printf("tid = %d, there is data in Ignite cache associated " + "with window number = %d\n",
						tid, windowNum);
			}

			// Loop through the data in the segment and send each measurement to
			// the Ignite server
			for (Object measurement : segment.getMainData()) {
				if (i++ % (sampleRate * secPerWindow) == 0) {
					windowNum++;
				}

				// Once we are sure the previous window with the same number was
				// processed for
				// that consumer, we cache our current window
				dataStreamer.addData(String.valueOf(tid + "_" + i), new MeasurementInfo(tid, windowNum, measurement));
			}
			dataStreamer.flush(); // Flush out all of the data to the cache
		}
	}
}
