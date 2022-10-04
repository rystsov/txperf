package io.vectorized.tx_subscribe;

import com.google.gson.Gson;
import java.io.*;
import java.util.HashMap;

import static spark.Spark.*;
import spark.*;

public class App
{
    public static class InitBody {
        public String experiment;
        public String server;
        public String brokers;
        public String source;
        public String target;
        public String group_id;
        public int partitions;
        public int[] range;
    }

    public static class OpsInfo {
        public long succeeded_ops = 0;
        public long failed_ops = 0;
        public long timedout_ops = 0;
        public long ticks = 0;
        public long empty_ticks = 0;
        public long produced = 0;

        public OpsInfo copy() {
            var value = new OpsInfo();
            value.succeeded_ops = succeeded_ops;
            value.failed_ops = failed_ops;
            value.timedout_ops = timedout_ops;
            value.ticks = ticks;
            value.empty_ticks = empty_ticks;
            value.produced = produced;
            return value;
        }
    }

    public static class Info extends OpsInfo {
        public boolean is_active;
        public boolean is_filling;
        public HashMap<String, OpsInfo> threads = new HashMap<>();
    }

    static enum State {
        FRESH,
        INITIALIZED,
        FILLED,
        FILLING,
        STARTED,
        STOPPED
    }

    public class JsonTransformer implements ResponseTransformer {
        private Gson gson = new Gson();
    
        @Override
        public String render(Object model) {
            return gson.toJson(model);
        }
    }

    State state = State.FRESH;
    
    InitBody params = null;
    Workload workload = null;

    void run() throws Exception {
        port(8080);

        get("/ping", (req, res) -> {
            res.status(200);
            return "";
        });

        get("/info", "application/json", (req, res) -> {
            try {
                var info = new Info();
                info.is_active = false;
                info.is_filling = false;
                info.failed_ops = 0;
                info.succeeded_ops = 0;
                info.timedout_ops = 0;
                info.ticks = 0;
                info.empty_ticks = 0;
                info.produced = 0;
                if (workload != null) {
                    info.is_active = workload.is_active;
                    info.is_filling = workload.is_filling;
                    info.threads = workload.get_ops_info();
                    for (String key : info.threads.keySet()) {
                        var value = info.threads.get(key);
                        info.succeeded_ops += value.succeeded_ops;
                        info.failed_ops += value.failed_ops;
                        info.timedout_ops += value.timedout_ops;
                        info.ticks += value.ticks;
                        info.empty_ticks += value.empty_ticks;
                        info.produced += value.produced;
                    }
                }
                return info;
            } catch (Exception e) {
                System.out.println(e);
                e.printStackTrace();
                throw e;
            }
        }, new JsonTransformer());

        post("/init", (req, res) -> {
            try {
                if (state != State.FRESH) {
                    throw new Exception("Unexpected state: " + state);
                }
                state = State.INITIALIZED;
                Gson gson = new Gson();
                System.out.println(req.body());
                params = gson.fromJson(req.body(), InitBody.class);
                File root = new File(params.experiment);
                if (!root.mkdir()) {
                    throw new Exception("Can't create folder: " + params.experiment);
                }
                workload = new Workload(params);
                res.status(200);
                return "";
            } catch (Exception e) {
                System.out.println(e);
                e.printStackTrace();
                throw e;
            }
        });

        post("/event/:name", (req, res) -> {
            var name = req.params(":name");
            workload.event(name);
            res.status(200);
            return "";
        });

        post("/startFilling", (req, res) -> {
            if (state != State.INITIALIZED) {
                throw new Exception("Unexpected state: " + state);
            }
            state = State.FILLING;
            try {
                workload.startFilling();
                res.status(200);
                return "";
            } catch (Exception e) {
                System.out.println(e);
                e.printStackTrace();
                throw e;
            }
        });

        post("/stopFilling", (req, res) -> {
            if (state != State.FILLING) {
                throw new Exception("Unexpected state: " + state);
            }
            state = State.FILLED;
            try {
                workload.stopFilling();
                res.status(200);
                return "";
            } catch (Exception e) {
                System.out.println(e);
                e.printStackTrace();
                throw e;
            }
        });
        
        post("/start", (req, res) -> {
            try {
                if (state != State.FILLED && state != State.INITIALIZED) {
                    throw new Exception("Unexpected state: " + state);
                }
                state = State.STARTED;
                workload.start();
                
                //curl -X POST http://127.0.0.1:8080/start -H 'Content-Type: application/json' -d '{"topic":"topic1","brokers":"127.0.0.1:9092"}'
                res.status(200);
                return "";
            } catch (Exception e) {
                System.out.println(e);
                e.printStackTrace();
                throw e;
            }
        });

        post("/stop", (req, res) -> {
            if (state != State.STARTED) {
                throw new Exception("Unexpected state: " + state);
            }
            state = State.STOPPED;
            workload.stop();
            //curl -X POST http://127.0.0.1:8080/start -H 'Content-Type: application/json' -d '{"topic":"topic1","brokers":"127.0.0.1:9092"}'
            res.status(200);
            return "";
        });
    }

    public static void main( String[] args ) throws Exception
    {
        new App().run();
    }
}
