package io.vectorized;
import java.io.*;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import java.util.HashSet;
import java.util.Collection;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import java.util.concurrent.CompletableFuture;


public class Workload {
    private static class TrackingRebalanceListener implements ConsumerRebalanceListener {
        public final Workload workload;
        public final int sid;

        public final HashSet<Integer> lost_partitions;
        public final HashSet<Integer> added_partitions;

        public TrackingRebalanceListener(Workload workload, int sid) {
            this.workload = workload;
            this.sid = sid;

            this.lost_partitions = new HashSet<>();
            this.added_partitions = new HashSet<>();
        }

        public void resetPartitions() {
            this.lost_partitions.clear();
            this.added_partitions.clear();
        }
        
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            for (var tp : partitions) {
                try {
                    workload.log(sid, "log\trevoke\t" + tp.partition());
                } catch (Exception e1) {}
                this.lost_partitions.add(tp.partition());
            }
        }
 
        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            for (var tp : partitions) {
                try {
                    workload.log(sid, "log\tlost\t" + tp.partition());
                } catch (Exception e1) {}
                this.lost_partitions.add(tp.partition());
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for (var tp : partitions) {
                try {
                    workload.log(sid, "log\tassign\t" + tp.partition());
                } catch (Exception e1) {}
                this.added_partitions.add(tp.partition());
            }
        }
    }
    private static interface StreamingCmd {}
    private static class StreamingExitCmd implements StreamingCmd {}
    private static class StreamingProducerInitCmd implements StreamingCmd {
        public long version;
    }
    private static class StreamingTxCmd implements StreamingCmd {
        public int partition;
        public long offset;
        public String key;
        public String value;
    }

    public volatile boolean is_active = false;

    private volatile App.InitBody args;
    private BufferedWriter opslog;

    private HashMap<Integer, App.OpsInfo> ops_info;
    private synchronized void succeeded(int thread_id) {
        ops_info.get(thread_id).succeeded_ops += 1;
    }
    private synchronized void failed(int thread_id) {
        ops_info.get(thread_id).failed_ops += 1;
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

    private long last_op_id = 0;
    private synchronized long get_op_id() {
        return ++this.last_op_id;
    }

    HashMap<Integer, Semaphore> produce_limiter;
    private HashMap<Integer, BlockingQueue<StreamingCmd>> streamingQueues;
    volatile ArrayList<Thread> producing_threads;
    volatile ArrayList<Thread> executing_threads;
    volatile Thread streaming_thread = null;

    static class ActivePartition {
        public long version;
        public long offset;
        public boolean failed = false;
        public boolean attached = false;
    }
    
    HashMap<Integer, Long> reset_partition;
    HashMap<Integer, ActivePartition> active_partition;


    public Workload(App.InitBody args) {
        this.args = args;
    }

    public void start() throws Exception {
        File root = new File(args.experiment, args.server);

        if (!root.mkdir()) {
            throw new Exception("Can't create folder: " + root);
        }

        is_active = true;
        past_us = 0;
        opslog = new BufferedWriter(new FileWriter(new File(new File(args.experiment, args.server), "workload.log")));
        
        ops_info = new HashMap<>();
        produce_limiter = new HashMap<>();
        streamingQueues = new HashMap<>();

        producing_threads = new ArrayList<>();
        executing_threads = new ArrayList<>();
        reset_partition = new HashMap<>();
        active_partition = new HashMap<>();
        int thread_id=0;

        for (int i=0;i<args.partitions;i++) {
            final int j = i;
            final var pid=thread_id++;
            produce_limiter.put(i, new Semaphore(10));
            producing_threads.add(new Thread(() -> { 
                try {
                    producingProcess(pid, j);
                } catch(Exception e) {
                    synchronized (this) {
                        System.out.println("=== error with producingProcess:" + pid);
                        System.out.println(e);
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            }));
        }

        for (int i=0;i<args.partitions;i++) {
            final int j = i;
            final var eid=thread_id++;
            streamingQueues.put(i, new LinkedBlockingQueue<>());
            ops_info.put(eid, new App.OpsInfo());
            executing_threads.add(new Thread(() -> { 
                try {
                    streamingExecutorProcess(eid, j);
                } catch(Exception e) {
                    synchronized (this) {
                        System.out.println("=== error with streamingExecutorProcess:" + eid);
                        System.out.println(e);
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            }));
        }

        {
            final var sid=thread_id++;
            streaming_thread = new Thread(() -> { 
                try {
                    streamingControlProcess(sid);
                } catch(Exception e) {
                    synchronized (this) {
                        System.out.println("=== error with streamingControlProcess:" + sid);
                        System.out.println(e);
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            });
        }

        for (var th : producing_threads) {
            th.start();
        }

        for (var th : executing_threads) {
            th.start();
        }

        streaming_thread.start();
    }

    public void stop() throws Exception {
        is_active = false;

        Thread.sleep(1000);
        if (opslog != null) {
            opslog.flush();
        }
        
        System.out.println("waiting for streaming_thread");
        if (streaming_thread != null) {
            streaming_thread.join();
        }

        System.out.println("waiting for executing_threads");
        for (int i=0;i<args.partitions;i++) {
            streamingQueues.get(i).add(new StreamingExitCmd());
        }
        synchronized (this) {
            for (int i=0;i<args.partitions;i++) {
                reset_partition.put(i, Long.MAX_VALUE);
            }
        }
        for (var th : executing_threads) {
            th.join();
        }
        
        System.out.println("waiting for producing_threads");
        for (int i=0;i<args.partitions;i++) {
            produce_limiter.get(i).release(100);
        }
        for (var th : producing_threads) {
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

    private void producingProcess(int pid, int partition) throws Exception {
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

        log(pid, "started\t" + args.server + "\tproducing\t" + partition);
    
        Producer<String, String> producer = null;
        boolean reset = false;

        while (is_active) {
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

            var oid = get_op_id();

            try {
                producer.send(new ProducerRecord<String, String>(args.source, partition, args.server, "" + oid)).get();
            } catch (Exception e1) {
                synchronized (this) {
                    System.out.println("=== error on send pid:" + pid);
                    System.out.println(e1);
                    e1.printStackTrace();
                }
                reset = true;
                continue;
            }

            produce_limiter.get(partition).acquire();
        }
    
        if (producer != null) {
            try {
                log(pid, "log\tproducer:close");
                producer.close(Duration.ofMillis(0));
                log(pid, "log\tproducer:close:ok");
            } catch (Exception e) { }
        }
    }

    private void streamingControlProcess(int sid) throws Exception {
        Properties cprops = new Properties();
        cprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args.brokers);
        cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cprops.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        cprops.put(ConsumerConfig.GROUP_ID_CONFIG, args.group_id);
        cprops.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        cprops.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        // default value: 540000
        cprops.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 60000);
        // default value: 60000
        cprops.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        // default value: 500
        cprops.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        // default value: 300000
        cprops.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 10000);
        // default value: 1000
        cprops.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
        // default value: 50
        cprops.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 0);
        // defaut value: 30000
        cprops.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        // default value: 100
        cprops.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        cprops.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        cprops.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        
        log(sid, "started\t" + args.server + "\tstreaming");

        Consumer<String, String> consumer = null;
        TrackingRebalanceListener tracker = null;
        HashMap<Integer, Long> producer_version = new HashMap<>();

        boolean reset = true;
        long version = 0;
        synchronized (this) {
            for (int i=0;i<args.partitions;i++) {
                reset_partition.put(i, version);
            }
        }

        while (is_active) {
            log(sid, "log\ttick");

            ConsumerRecords<String, String> records = null;

            synchronized (this) {
                for (var partition : reset_partition.keySet()) {
                    if (!producer_version.containsKey(partition)) {
                        continue;
                    }
                    if (producer_version.get(partition) <= reset_partition.get(partition)) {
                        log(sid, "log\treset:detected:stream:" + partition + ":ver:" + reset_partition.get(partition));
                        reset = true;
                    }
                }
            }

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

                if (tracker != null) {
                    tracker = null;
                }

                synchronized (this) {
                    for (int i=0;i<args.partitions;i++) {
                        log(sid, "log\treset:" + i + ":" + version);
                        reset_partition.put(i, version);
                    }
                }

                try {
                    log(sid, "constructing\tstreaming");
                    consumer = new KafkaConsumer<>(cprops);
                    tracker = new TrackingRebalanceListener(this, sid);
                    consumer.subscribe(Collections.singleton(args.source), tracker);
                    log(sid, "constructed");
                } catch (Exception e1) {
                    log(sid, "err");
                    synchronized (this) {
                        System.out.println("=== error on KafkaConsumer ctor sid:" + sid);
                        System.out.println(e1);
                        e1.printStackTrace();
                    }
                    continue;
                }

                if (tracker.added_partitions.size() != 0) {
                    log(sid, "log\tstreaming\tfresh tracker should be empty");
                    continue;
                }

                reset = false;
            }
            
            log(sid, "log\ttack");
            HashSet<Integer> skip_partition = new HashSet<>();
            synchronized (this) {
                for (var partition : producer_version.keySet()) {
                    if (!active_partition.containsKey(partition)) {
                        skip_partition.add(partition);
                        continue;
                    }
                    var info = active_partition.get(partition);
                    if (info.failed) {
                        skip_partition.add(partition);
                        continue;
                    }
                    if (info.version != producer_version.get(partition)) {
                        skip_partition.add(partition);
                        continue;
                    }
                    if (!info.attached) {
                        var tp = new TopicPartition(args.source, partition);
                        consumer.seek(tp, info.offset);
                        info.attached = true;
                    }
                }
            }
            records = consumer.poll(Duration.ofMillis(10000));
            for (var partition : tracker.added_partitions) {
                skip_partition.add(partition);
            }
            synchronized (this) {
                for (var partition : tracker.lost_partitions) {
                    log(sid, "log\treset:" + partition + ":" + version);
                    reset_partition.put(partition, version);
                    producer_version.remove(partition);
                }
            }
            log(sid, "log\ttock");

            var it = records.iterator();
            while (it.hasNext()) {
                var record = it.next();

                StreamingTxCmd tx = new StreamingTxCmd();
                tx.partition = record.partition();
                tx.key = record.key();
                tx.offset = record.offset();
                tx.value = record.value();

                if (skip_partition.contains(record.partition())) {
                    continue;
                }

                streamingQueues.get(tx.partition).add(tx);
            }

            if (tracker.added_partitions.size() == 0) {
                tracker.resetPartitions();
                continue;
            }
            
            version++;
            log(sid, "log\tassignments:begin");
            for(int partition : tracker.added_partitions) {
                log(sid, "log\tassignment:" + partition);
                StreamingProducerInitCmd cmd = new StreamingProducerInitCmd();
                cmd.version = version;
                producer_version.put(partition, version);
                streamingQueues.get(partition).add(cmd);
            }
            log(sid, "log\tassignments:end");
            tracker.resetPartitions();
        }

        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) { }
        }
    }

    private void streamingExecutorProcess(int eid, int partition) throws Exception {
        log(eid, "started\t" + args.server + "\texecuting\t" + partition);
        
        var tp = new TopicPartition(args.source, partition);
        var queue = streamingQueues.get(partition);

        Producer<String, String> producer = null;
        long version = -1;

        try {
            while (is_active) {
                var cmd = queue.take();
                if (cmd instanceof StreamingExitCmd) {
                    return;
                }
    
                if (cmd instanceof StreamingProducerInitCmd) {
                    var init = (StreamingProducerInitCmd)cmd;
                    
                    log(eid, "log\tcreating:stream:" + partition + ":" + init.version + ":init");
                    
                    while (true) {
                        synchronized (this) {
                            if (init.version <= reset_partition.get(partition)) {
                                log(eid, "log\tcreating:stream:" + partition + ":" + init.version + ":skip");
                                break;
                            }
                        }
    
                        if (producer != null) {
                            try {
                                log(eid, "log\tclosing:stream:" + partition + ":" + version);
                                producer.close(Duration.ofMillis(0));
                                log(eid, "log\tclosing:stream:" + partition + ":" + version + ":ok");
                            } catch(Exception e1) {
                                log(eid, "log\tclosing:stream:" + partition + ":" + version + ":err");
                                synchronized (this) {
                                    System.out.println("=== error on closing:stream:" + partition + ":" + version + " eid:" + eid);
                                    System.out.println(e1);
                                    e1.printStackTrace();
                                }
                                continue;
                            }
                            producer = null;
                            version = -1;
                        }
    
                        
                        try {
                            // TODO: do double fencing
                            log(eid, "log\tcreating:stream:" + partition + ":" + init.version);
                            var new_producer = createProducer(partition);
                            log(eid, "log\tcreating:stream:" + partition + ":" + init.version + ":ok");
                            
                            var info = new ActivePartition();
                            info.failed = false;
                            info.version = init.version;
                            info.offset = 0;
                            var offsets = getOffsets(eid, args.group_id);
                            if (offsets == null) {
                                continue;
                            }
                            if (offsets.containsKey(tp)) {
                                info.offset = offsets.get(tp).offset();
                            }
    
                            synchronized (this) {
                                active_partition.put(partition, info);
                            }
                            version = init.version;
                            producer = new_producer;
    
                            break;
                        } catch(Exception e1) {
                            log(eid, "log\tcreating:stream:" + partition + ":" + init.version + ":err");
                            synchronized (this) {
                                System.out.println("=== error on creating:stream:" + partition + ":" + init.version + " eid:" + eid);
                                System.out.println(e1);
                                e1.printStackTrace();
                            }
                            Thread.sleep(1000);
                        }
                    }
    
                    continue;
                }
    
                // TODO: mark consumed, kick production if below 50
                produce_limiter.get(partition).release();
    
                var tx = (StreamingTxCmd)cmd;
    
                var is_recovery = false;
    
                while (true) {
                    synchronized (this) {
                        if (version <= reset_partition.get(partition)) {
                            log(eid, "log\tprocessing:stream:" + partition + ":" + version + ":skip");
                            break;
                        }
                    }
    
                    if (producer == null) {
                        try {
                            log(eid, "log\trecovery:stream:" + partition + ":" + version);
                            producer = createProducer(partition);
                            log(eid, "log\trecovery:stream:" + partition + ":" + version + ":ok");
                        } catch(Exception e1) {
                            log(eid, "log\tcreating:stream:" + partition + ":" + version + ":err");
                            synchronized (this) {
                                System.out.println("=== error on creating:stream:" + partition + ":" + version + " eid:" + eid);
                                System.out.println(e1);
                                e1.printStackTrace();
                            }
                            Thread.sleep(1000);
                            continue;
                        }
                    }
    
                    if (is_recovery) {
                        log(eid, "log\trecovery:tx:" + partition + ":" + tx.offset);
                        var offsets = getOffsets(eid, args.group_id);
                        if (offsets == null) {
                            continue;
                        }
                        if (offsets.containsKey(tp)) {
                            if (offsets.get(tp).offset() > tx.offset + 1) {
                                // oops concurrent write
                                log(eid, "log\trecovery:tx:" + partition + ":" + tx.offset + ":concurrent");
                                is_recovery = false;
                                break;
                            }
                            if (offsets.get(tp).offset() == tx.offset + 1) {
                                log(eid, "log\trecovery:tx:" + partition + ":" + tx.offset + ":recovered");
                                // current transaction has passed
                                is_recovery = false;
                                break;
                            }
                        }
                        // current transaction hasn't passed
                        log(eid, "log\trecovery:tx:" + partition + ":" + tx.offset + ":retry");
                        is_recovery = false;
                    }
                    
                    try {
                        log(eid, "tx");
                        producer.beginTransaction();
                        producer.send(new ProducerRecord<String, String>(args.target, partition, tx.key, tx.value));
                        var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
                        offsets.put(new TopicPartition(args.source, partition), new OffsetAndMetadata(tx.offset + 1));
                        producer.sendOffsetsToTransaction(offsets, args.group_id);
                    } catch (Exception e1) {
                        synchronized (this) {
                            System.out.println("=== error on send or sendOffsetsToTransaction eid:" + eid);
                            System.out.println(e1);
                            e1.printStackTrace();
                        }
        
                        try {
                            log(eid, "brt");
                            producer.abortTransaction();
                            log(eid, "ok");
                            failed(eid);
                        } catch (Exception e2) {
                            log(eid, "err");
                            synchronized (this) {
                                System.out.println("=== error on abort eid:" + eid);
                                System.out.println(e2);
                                e2.printStackTrace();
                            }
                        }
    
                        try {
                            log(eid, "log\tclosing:stream:begin:" + partition + ":" + version);
                            producer.close(Duration.ofMillis(0));
                            log(eid, "log\tclosing:stream:begin:" + partition + ":" + version + ":ok");
                        } catch(Exception e2) {
                            log(eid, "log\tclosing:stream:begin:" + partition + ":" + version + ":err");
                            synchronized (this) {
                                System.out.println("=== error on closing:stream:" + partition + ":" + version + " eid:" + eid);
                                System.out.println(e2);
                                e1.printStackTrace();
                            }
                        }
                        producer = null;
                        is_recovery = false;
                        continue;
                    }
    
                    try {
                        log(eid, "cmt");
                        producer.commitTransaction();
                        log(eid, "ok");
                        succeeded(eid);
                    } catch (Exception e1) {
                        log(eid, "err");
                        failed(eid);
                        synchronized(this) {
                            System.out.println("=== error on commit eid:" + eid);
                            System.out.println(e1);
                            e1.printStackTrace();
                        }
                        try {
                            log(eid, "log\tclosing:stream:commit:" + partition + ":" + version);
                            producer.close(Duration.ofMillis(0));
                            log(eid, "log\tclosing:stream:commit:" + partition + ":" + version + ":ok");
                        } catch(Exception e2) {
                            log(eid, "log\tclosing:stream:commit:" + partition + ":" + version + ":err");
                            synchronized (this) {
                                System.out.println("=== error on closing:stream:" + partition + ":" + version + " eid:" + eid);
                                System.out.println(e2);
                                e1.printStackTrace();
                            }
                        }
                        is_recovery = true;
                        continue;
                    }
    
                    break;
                }   
            }
        } catch (Exception e0) {
            synchronized (this) {
                System.out.println("=== imposible error partition:" + partition + ":" + version + " eid:" + eid);
                System.out.println(e0);
                e0.printStackTrace();
            }

            log(eid, "fatal");
        }
    }

    private Producer<String, String> createProducer(int partition) {
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
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-consume-" + args.source + "-partition-" + partition);

        producer =  new KafkaProducer<>(props);
        producer.initTransactions();
        return producer;
    }

    private Map<TopicPartition, OffsetAndMetadata> getOffsets(int id, String group_id) {
        Map<TopicPartition, OffsetAndMetadata> result = null;
        
        AdminClient admin = null;
        try {
            log(id, "log\tadmin:fetch");
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, args.brokers);
            // default value: 600000
            props.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 60000);
            // default value: 300000
            props.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, 10000);
            // default value: 1000
            props.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
            // default value: 50
            props.put(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
            // default value: 30000
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
            // default value: 100
            props.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, 0);
            // default.api.timeout.ms 60000
            props.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
            // request.timeout.ms 30000
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
            // default value: 30000
            props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, 10000);
            // default value: 10000
            props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, 10000);
            props.put(AdminClientConfig.RETRIES_CONFIG, 0);
            props.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, 10000);

            admin = AdminClient.create(props);
            var f = admin.listConsumerGroupOffsets(group_id);
            result = f.partitionsToOffsetAndMetadata().get();
            log(id, "log\tadmin:fetch:ok");
        } catch(Exception e1) {
            synchronized (this) {
                System.out.println("=== error on AdminClient::listConsumerGroupOffsets thread:" + id);
                System.out.println(e1);
                e1.printStackTrace();
            }
            try {
                log(id, "log\tadmin:fetch:err");
            } catch (Exception e2) {}
        }

        if (admin != null) {
            try {
                log(id, "log\tadmin:close");
                admin.close();
                log(id, "log\tadmin:close:ok");
            } catch (Exception e1) {
                try {
                    log(id, "log\tadmin:close:err");
                } catch(Exception e2) {}
            }
        }

        return result;
    }
}