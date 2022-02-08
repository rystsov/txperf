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
import java.util.Random;
import java.util.UUID;
import java.nio.charset.Charset;

public class Workload {
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
        log(-1, "event\t" + name);
    }

    private volatile ArrayList<Thread> threads;

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

        int thread_id=0;
        threads = new ArrayList<>();
        for (int i=0;i<this.args.settings.producers;i++) {
            final var j=thread_id;
            thread_id+=2;
            ops_info.put(j, new App.OpsInfo());
            ops_info.put(j+1, new App.OpsInfo());
            threads.add(new Thread(() -> { 
                try {
                    transferProcess(j);
                } catch(Exception e) {
                    synchronized(this) {
                        System.out.println("=== error on transferProcess wid:" + j);
                        System.out.println(e);
                        e.printStackTrace();
                    }
                    
                    try {
                        opslog.flush();
                        opslog.close();
                    } catch(Exception e2) {}
                    System.exit(1);
                }
            }));
        }
        
        for (var th : threads) {
            th.start();
        }
    }

    public void stop() throws Exception {
        is_active = false;
        
        Thread.sleep(1000);
        if (opslog != null) {
            opslog.flush();
        }

        for (var th : threads) {
            th.join();
        }
        if (opslog != null) {
            opslog.flush();
            opslog.close();
        }
    }

    public synchronized HashMap<String, App.OpsInfo> get_ops_info() {
        HashMap<String, App.OpsInfo> result = new HashMap<>();
        for (Integer key : ops_info.keySet()) {
            result.put("" + key, ops_info.get(key).copy());
        }
        return result;
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

    private void transferProcess(int wid1) throws Exception {
        var random = new Random();

        Properties[] props = new Properties[2];
        
        for (int i=0;i<props.length;i++) {
            props[i] = new Properties();
            props[i].put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args.brokers);
            props[i].put(ProducerConfig.ACKS_CONFIG, "all");
            props[i].put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props[i].put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            
            // default value: 600000
            props[i].put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 60000);
            // default value: 120000
            props[i].put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
            // default value: 0
            props[i].put(ProducerConfig.LINGER_MS_CONFIG, 0);
            // default value: 60000
            props[i].put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
            // default value: 1000
            props[i].put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
            // default value: 50
            props[i].put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
            // default value: 30000
            props[i].put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
            // default value: 100
            props[i].put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 0);
            // default value: 300000
            props[i].put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 10000);
            // default value: 300000
            props[i].put(ProducerConfig.METADATA_MAX_IDLE_CONFIG, 10000);
            
            props[i].put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
            props[i].put(ProducerConfig.RETRIES_CONFIG, args.settings.retries);
            props[i].put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            props[i].put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
        }
    
    
        KafkaProducer[] producers = new KafkaProducer[2];
    
        log(wid1, "started\t" + args.server + "\ttransferring");
        log(wid1+1, "started\t" + args.server + "\ttransferring");

        int j=0;
        while (is_active) {
            j = (j + 1) % props.length;
            try {
                if (producers[j] == null) {
                    log(wid1 + j, "constructing");
                    producers[j] = new KafkaProducer<String,String>(props[j]);
                    producers[j].initTransactions();
                    log(wid1 + j, "constructed");
                    continue;
                }
            } catch (Exception e1) {
                log(wid1 + j, "err");
                synchronized (this) {
                    System.out.println("=== error on constructing wid:" + (wid1 + j));
                    System.out.println(e1);
                    e1.printStackTrace();
                }
                failed(wid1 + j);
                try {
                    if (producers[j] != null) {
                        producers[j].close();
                    }
                } catch(Exception e2) { }
                producers[j] = null;
                continue;
            }
    
            var acc1 = "acc" + random.nextInt(this.args.accounts);
            var acc2 = acc1;
            while (acc1.equals(acc2)) {
                acc2 = "acc" + random.nextInt(this.args.accounts);
            }

            var part1 = randomString(random, 1024);
            var part2 = randomString(random, 1024);

            var producer1 = (KafkaProducer<String,String>)producers[j];

            log(wid1 + j, "tx");
            producer1.beginTransaction();

            try {
                var f1 = producer1.send(new ProducerRecord<String, String>(acc1, null, part1));
                var f2 = producer1.send(new ProducerRecord<String, String>(acc2, null, part2));
                f1.get();
                f2.get();
            } catch (Exception e1) {
                synchronized(this) {
                    System.out.println("=== error on produce => aborting tx wid:" + (wid1 + j));
                    System.out.println(e1);
                    e1.printStackTrace();
                }
    
                try {
                    log(wid1 + j, "brt");
                    producer1.abortTransaction();
                    log(wid1 + j, "ok");
                    failed(wid1 + j);
                } catch (Exception e2) {
                    synchronized(this) {
                        System.out.println("=== error on abort => reset producer wid:" + (wid1 + j));
                        System.out.println(e2);
                        e2.printStackTrace();
                    }
                    log(wid1 + j, "err");
                    try {
                        producer1.close();
                    } catch (Exception e3) {}
                    producers[j] = null;
                }
    
                continue;
            }
    
            try {
                log(wid1 + j, "cmt");
                producer1.commitTransaction();
                log(wid1 + j, "ok");
                succeeded(wid1 + j);
            } catch (Exception e1) {
                synchronized(this) {
                    System.out.println("=== error on commit => reset producer wid:" + (wid1 + j));
                    System.out.println(e1);
                    e1.printStackTrace();
                }
                log(wid1 + j, "err");
                failed(wid1 + j);
                try {
                    producer1.close();
                } catch (Exception e3) {}
                producers[j] = null;
            }
        }

        for (int i=0;i<producers.length;i++) {
            if (producers[i] != null) {
                try {
                    producers[i].close();
                } catch (Exception e) { }
            }
        }
    }
}