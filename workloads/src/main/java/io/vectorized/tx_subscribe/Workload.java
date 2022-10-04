package io.vectorized.tx_subscribe;
import java.io.*;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.lang.Thread;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.HashSet;
import java.util.Collection;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.Random;
import java.nio.charset.Charset;


public class Workload {
    public volatile boolean is_active = false;

    public volatile boolean is_filling = false;
    private volatile App.InitBody args;
    private BufferedWriter opslog;

    private HashMap<Integer, App.OpsInfo> ops_info;
    private synchronized void succeeded(int thread_id) {
        ops_info.get(thread_id).succeeded_ops += 1;
    }
    private synchronized void failed(int thread_id) {
        ops_info.get(thread_id).failed_ops += 1;
    }
    private synchronized void tick(int thread_id) {
        ops_info.get(thread_id).ticks += 1;
    }
    private synchronized void produced(int thread_id) {
        ops_info.get(thread_id).produced += 1;
    }
    private synchronized void emptyTick(int thread_id) {
        ops_info.get(thread_id).empty_ticks += 1;
        ops_info.get(thread_id).ticks += 1;
    }

    private long past_us;
    private synchronized void log(int thread_id, String message) throws Exception {
        var now_us = System.nanoTime() / 1000;
        if (now_us < past_us) {
            throw new Exception("Time cant go back, observed: " + now_us + " after: " + past_us);
        }
        opslog.write("" + thread_id +
                        "\t" + (now_us - past_us) +
                        "\t" + message + "\n");
        past_us = now_us;
    }
    public void event(String name) throws Exception {
        opslog.flush();
        log(-1, "event\t" + name);
        opslog.flush();
    }

    private int last_pid = 0;
    private synchronized int get_pid() {
        return ++this.last_pid;
    }

    volatile Thread filling_thread;
    volatile ArrayList<Thread> streaming_threads;

    public Workload(App.InitBody args) throws Exception {
        this.args = args;
        File root = new File(args.experiment, args.server);

        if (!root.mkdir()) {
            throw new Exception("Can't create folder: " + root);
        }

        past_us = 0;
        opslog = new BufferedWriter(new FileWriter(new File(new File(args.experiment, args.server), "workload.log")));

        ops_info = new HashMap<>();
    }

    private String randomString(Random random, int length) {
        byte[] array = new byte[length];
        random.nextBytes(array);
        for (int i=0;i<length;i++) {
            // 32 is ' ', 126 is '~', generating all in between
            array[i] = (byte)(32 + array[i]%(126 - 32));
        }
        return new String(array, Charset.forName("UTF-8"));
    }

    public void startFilling() throws Exception {
        is_filling = true;

        {
            final var fid=get_pid();
            ops_info.put(fid, new App.OpsInfo());
            filling_thread = new Thread(() -> { 
                try {
                    fillingProcess(fid);
                } catch(Exception e) {
                    synchronized (this) {
                        System.out.println("=== error with fillingProcess:" + fid);
                        System.out.println(e);
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            });
        }

        filling_thread.start();
    }

    public void stopFilling() throws Exception {
        is_filling = false;
        filling_thread.join();
        filling_thread = null;
    }

    private void fillingProcess(int pid) throws Exception {
        var random = new Random();
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args.brokers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        
        // default value: 600000
        props.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 60000);
        // default value: 120000
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        // default value: 0
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        // default value: 60000
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        // default value: 1000
        props.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
        // default value: 50
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
        // default value: 30000
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        // default value: 100
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 0);
        // default value: 300000
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 10000);
        // default value: 300000
        props.put(ProducerConfig.METADATA_MAX_IDLE_CONFIG, 10000);

        log(pid, "started\t" + args.server + "\tproducing");
    
        Producer<String, String> producer = null;
        boolean reset = false;

        while (is_filling) {
            if (reset) {
                try {
                    log(pid, "log\tproducer:close");
                    producer.close(Duration.ofMillis(0));
                    log(pid, "log\tproducer:close:ok");
                    Thread.sleep(1000);
                } catch (Exception e3) {}
                producer = null;
                reset = false;
            }

            try {
                if (producer == null) {
                    log(pid, "constructing");
                    producer = new KafkaProducer<>(props);
                    log(pid, "constructed");
                    continue;
                }
            } catch (Exception e1) {
                log(pid, "err");
                synchronized (this) {
                    System.out.println("=== error on KafkaProducer ctor pid:" + pid);
                    System.out.println(e1);
                    e1.printStackTrace();
                }
                reset = true;
                continue;
            }

            var fss = new ArrayList<ArrayList<Future<RecordMetadata>>>();

            for (int j=0;j<10000;j++) {
                var fs = new ArrayList<Future<RecordMetadata>>();
                for (int i=0;i<args.partitions;i++) {
                    fs.add(producer.send(new ProducerRecord<String, String>(args.source + i, "", randomString(random, 1024))));
                }
                fss.add(fs);
            }

            for (var fs : fss) {
                try {
                    for (var f : fs) {
                        f.get();
                    }
                } catch (Exception e1) {
                    synchronized (this) {
                        System.out.println("=== error on send pid:" + pid);
                        System.out.println(e1);
                        e1.printStackTrace();
                    }
                    reset = true;
                    break;
                }
                produced(pid);
            }
            log(pid, "+1000");
        }
    
        if (producer != null) {
            try {
                log(pid, "log\tproducer:close");
                producer.close(Duration.ofMillis(0));
                log(pid, "log\tproducer:close:ok");
            } catch (Exception e) { }
        }
    }

    

    public void start() throws Exception {
        is_active = true;

        streaming_threads = new ArrayList<>();

        for (int i : args.range) {
            final var sid=get_pid();
            final var r = i;
            ops_info.put(sid, new App.OpsInfo());
            streaming_threads.add(new Thread(() -> { 
                try {
                    streamingProcess(sid, r);
                } catch(Exception e) {
                    synchronized (this) {
                        System.out.println("=== error with streamingExecutorProcess:" + sid);
                        System.out.println(e);
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            }));
        }

        for (var th : streaming_threads) {
            th.start();
        }
    }

    public void stop() throws Exception {
        is_active = false;
        is_filling = false;

        Thread.sleep(1000);
        if (opslog != null) {
            opslog.flush();
        }
        
        System.out.println("waiting for filling_thread");
        if (filling_thread != null) {
            filling_thread.join();
        }

        System.out.println("waiting for streaming_threads");
        for (var th : streaming_threads) {
            th.join();
        }
        
        if (opslog != null) {
            opslog.flush();
            opslog.close();
        }
    }

    public HashMap<String, App.OpsInfo> get_ops_info() {
        try {
            opslog.flush();
        } catch (Exception e1) {}
        synchronized(this) {
            HashMap<String, App.OpsInfo> result = new HashMap<>();
            for (Integer key : ops_info.keySet()) {
                result.put("" + key, ops_info.get(key).copy());
            }
            return result;
        }
    }

    private void streamingProcess(int sid, int r) throws Exception {
        log(sid, "started\t" + args.server + "\tstreaming");

        Consumer<String, String> consumer = null;
        Producer<String, String> producer = null;
        ConsumerRecords<String, String> records = null;

        boolean reset = true;

        while (is_active) {
            log(sid, "log\ttick");

            if (reset) {
                if (consumer != null) {
                    try {
                        log(sid, "log\tclosing:consumer");
                        consumer.close();
                        log(sid, "log\tclosing:consumer:ok");
                    } catch (Exception e2) {
                        log(sid, "log\tclosing:consumer:err");
                    }
                    consumer = null;
                }

                if (producer != null) {
                    try {
                        producer.close();
                    } catch (Exception e2) {}
                    producer = null;
                }     

                reset = false;
            }

            if (producer == null) {
                try {
                    log(sid, "constructing\tproducer");
                    producer = createProducer();
                    log(sid, "constructed");
                } catch (Exception e1) {
                    reset = true;
                    log(sid, "err");
                    synchronized (this) {
                        System.out.println("=== error on KafkaProducer ctor sid:" + sid);
                        System.out.println(e1);
                        e1.printStackTrace();
                    }
                    continue;
                }
            }

            if (consumer == null) {
                try {
                    log(sid, "constructing\tconsumer");
                    consumer = createConsumer(r);
                    consumer.subscribe(Collections.singleton(args.source + r));
                    log(sid, "constructed");
                } catch (Exception e1) {
                    reset = true;
                    log(sid, "err");
                    synchronized (this) {
                        System.out.println("=== error on KafkaConsumer ctor sid:" + sid);
                        System.out.println(e1);
                        e1.printStackTrace();
                    }
                    continue;
                }
            }
            
            log(sid, "log\ttack");
            records = consumer.poll(Duration.ofMillis(10000));
            if (records.count()==0) {
                emptyTick(sid);
            } else {
                tick(sid);
            }
            log(sid, "log\ttock");

            var it = records.iterator();
            while (it.hasNext()) {
                var record = it.next();

                try {
                    log(sid, "tx");
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<String, String>(args.target + r, 0, record.key(), record.value()));
                    var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
                    offsets.put(new TopicPartition(args.source + r, 0), new OffsetAndMetadata(record.offset() + 1));
                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                } catch (Exception e1) {
                    synchronized (this) {
                        System.out.println("=== error on send or sendOffsetsToTransaction sid:" + sid);
                        System.out.println(e1);
                        e1.printStackTrace();
                    }

                    try {
                        log(sid, "brt");
                        producer.abortTransaction();
                        log(sid, "ok");
                        failed(sid);
                    } catch (Exception e2) {
                        log(sid, "err");
                        synchronized (this) {
                            System.out.println("=== error on abort sid:" + sid);
                            System.out.println(e2);
                            e2.printStackTrace();
                        }
                    }
                    reset = true;
                    break;
                }
               
                try {
                    log(sid, "cmt");
                    producer.commitTransaction();
                    log(sid, "ok");
                    succeeded(sid);
                } catch (Exception e1) {
                    log(sid, "err");
                    failed(sid);
                    synchronized(this) {
                        System.out.println("=== error on commit sid:" + sid);
                        System.out.println(e1);
                        e1.printStackTrace();
                    }
                    reset = true;
                    break;
                }
            }
        }

        if (producer != null) {
            try {
                producer.close();
            } catch (Exception e) { }
        }
        
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) { }
        }
    }

    private Consumer<String, String> createConsumer(int r) {
        Properties cprops = new Properties();
        cprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args.brokers);
        cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cprops.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        cprops.put(ConsumerConfig.GROUP_ID_CONFIG, args.group_id + r);
        cprops.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        cprops.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        cprops.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 60000);
        cprops.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        cprops.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        cprops.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 10000);
        cprops.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
        cprops.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 0);
        cprops.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        cprops.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        cprops.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        cprops.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        
        return new KafkaConsumer<>(cprops);
    }
    
    private Producer<String, String> createProducer() {
        Producer<String, String> producer = null;
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args.brokers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        
        // default value: 600000
        props.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 60000);
        // default value: 120000
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        // default value: 0
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        // default value: 60000
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        // default value: 1000
        props.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
        // default value: 50
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
        // default value: 30000
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        // default value: 100
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 0);
        // default value: 300000
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 10000);
        // default value: 300000
        props.put(ProducerConfig.METADATA_MAX_IDLE_CONFIG, 10000);
        
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());

        producer =  new KafkaProducer<>(props);
        producer.initTransactions();
        return producer;
    }
}