package io.vectorized.base_money;
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

    private HashMap<Integer, Boolean> should_reset;
    private HashMap<Integer, Long> last_success_us;
    private synchronized void progress(int thread_id) {
        last_success_us.put(thread_id, System.nanoTime() / 1000);
    }
    private synchronized void tick(int thread_id) {
        var now_us = Math.max(last_success_us.get(thread_id), System.nanoTime() / 1000);
        if (now_us - last_success_us.get(thread_id) > 10 * 1000 * 1000) {
            should_reset.put(thread_id, true);
            last_success_us.put(thread_id, now_us);
        }
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
        
        should_reset = new HashMap<>();
        last_success_us = new HashMap<>();
        ops_info = new HashMap<>();

        int thread_id=0;
        threads = new ArrayList<>();
        for (int i=0;i<this.args.settings.producers;i++) {
            final var j=thread_id++;
            should_reset.put(j, false);
            last_success_us.put(j, -1L);
            ops_info.put(j, new App.OpsInfo());
            threads.add(new Thread(() -> { 
                try {
                    transferProcess(j);
                } catch(Exception e) {
                    try {
                        opslog.flush();
                        opslog.close();
                    } catch(Exception e2) {}

                    synchronized (this) {
                        System.out.println("=== error on transferProcess wid:" + j);
                        System.out.println(e);
                        e.printStackTrace();
                        System.exit(1);
                    }
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

    private void transferProcess(int wid) throws Exception {
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
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
    
    
        Producer<String, String> producer = null;
    
        log(wid, "started\t" + args.server + "\ttransferring");
    
        while (is_active) {
            tick(wid);
    
            synchronized(this) {
                if (should_reset.get(wid)) {
                    should_reset.put(wid, false);
                    if (producer != null) {
                        try {
                            producer.close();
                            producer = null;
                        } catch(Exception e) {}
                    }
                }
            }
    
            try {
                if (producer == null) {
                    log(wid, "constructing");
                    producer = new KafkaProducer<>(props);
                    log(wid, "constructed");
                    continue;
                }
            } catch (Exception e1) {
                log(wid, "err");
                synchronized (this) {
                    System.out.println("=== error on constructing wid:" + wid);
                    System.out.println(e1);
                    e1.printStackTrace();
                }
                failed(wid);
                try {
                    if (producer != null) {
                        producer.close();
                    }
                } catch(Exception e2) { }
                producer = null;
                continue;
            }
    
            var acc1 = "acc" + random.nextInt(this.args.accounts);
            var acc2 = acc1;
            while (acc1.equals(acc2)) {
                acc2 = "acc" + random.nextInt(this.args.accounts);
            }

            log(wid, "tx");

            var part1 = randomString(random, 1024);
            var part2 = randomString(random, 1024);

            try {
                var f1 = producer.send(new ProducerRecord<String, String>(acc1, null, part1));
                var f2 = producer.send(new ProducerRecord<String, String>(acc2, null, part2));
                f1.get();
                f2.get();
            } catch (Exception e1) {
                synchronized (this) {
                    System.out.println("=== error on produce wid:" + wid);
                    System.out.println(e1);
                    e1.printStackTrace();
                }

                log(wid, "err");
                continue;
            }

            log(wid, "ok");

            progress(wid);
            succeeded(wid);
        }
    
        if (producer != null) {
            try {
                producer.close();
            } catch (Exception e) { }
        }
    }
}