package com.sundear.day02;

import com.sun.imageio.plugins.common.I18N;
import com.sundear.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Flink06_Source_MySource {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //自定义数据读取数据
        DataStreamSource<WaterSensor> waterSensorDSS = env.addSource(new MySource("hadoop102", 9999));


        //打印结果
        waterSensorDSS.print();

        //启动任务
        env.execute();
    }

    //自定义数据源
    public static class MySource implements SourceFunction<WaterSensor>{

        //host
        private String hostName;

        //port
        private int port;


        Socket socket = null;

        BufferedReader reader = null;


        private boolean running = true;

        //无参构造器
        public MySource() {
        }

        //构造器
        public MySource(String hostName, int port) {
            this.hostName = hostName;
            this.port = port;
        }

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {

            //创建socket对象
            socket = new Socket(hostName, port);
            //创建输入流
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            //按行读取数据
            String line = reader.readLine();


            while (running && line != null) {
                String[] split = line.split(",");
                ctx.collect(new WaterSensor(
                        split[0],
                        Long.valueOf(split[1]),
                        Integer.parseInt(split[2])
                ));
                line = reader.readLine();
            }

        }

        @Override
        public void cancel() {
            running = false;
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}
