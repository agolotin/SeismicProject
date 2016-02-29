package main.java.edu.byu.seismicproject.consumer;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The partition offset are stored in an external storage. In this case in a file system.
 */
public class OffsetManager {


    private String storagePrefix;
    private int tid;

    public OffsetManager(String storagePrefix, int tid) {
        this.storagePrefix = storagePrefix;
        this.tid = tid;
    }
    
    public String getStorageName() {
    	return this.storagePrefix;
    }

    /**
     * Overwrite the offset for the topic in an external storage.
     * @param tid 
     *
     * @param topic     - Topic name.
     * @param partition - Partition of the topic.
     * @param offset    - offset to be stored.
     */
    void saveOffsetInExternalStore(String topic, int partition, long offset) {

        try {

            FileWriter writer = new FileWriter(storageName(topic, partition, tid), false);

            BufferedWriter bufferedWriter = new BufferedWriter(writer);
            bufferedWriter.write(offset + "");
            bufferedWriter.flush();
            bufferedWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * @return he last offset + 1 for the provided topic and partition.
     */
    @SuppressWarnings("resource")
    long readOffsetFromExternalStore(String topic, int partition) {

        try {
        	Path path = Paths.get(storageName(topic, partition, tid));
        	if (path.toFile().exists()) {
				Stream<String> stream = Files.lines(path);

				return Long.parseLong(stream.collect(Collectors.toList()).get(0)) + 1;
        	}
        	else {
        		path.toFile().createNewFile();
        	}

        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0;
    }

    private String storageName(String topic, int partition, int tid) {
        return storagePrefix + "-" + topic + "-" + partition + "-" + tid;
    }

}