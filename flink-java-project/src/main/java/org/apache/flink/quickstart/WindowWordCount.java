package org.apache.flink.quickstart;

import akka.stream.impl.ReducerState;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.QueryableStateStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Schema;
import scala.Int;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class WindowWordCount {

    private static final String topic = "words";

    public static void main(String[] args) throws Exception{

        //获取当前环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /** 通过socket来读取消息
        //  数据流输入和任务执行
        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        **/

        /**  asQueryableState()方法实现对外暴露状态

        //ReducingState
        ReducingStateDescriptor<Tuple2<String, List<Integer>>> stateDescriptor =
                new ReducingStateDescriptor<Tuple2<String, List<Integer>>>(
                        "wordCounts",
                        new ReduceFunction<Tuple2<String, List<Integer>>>() {
                            @Override
                            public Tuple2<String, List<Integer>> reduce(Tuple2<String, List<Integer>> t2, Tuple2<String, List<Integer>> t1) throws Exception {
                                List<Integer> res = t1.f1;
                                res.addAll(t2.f1);
                                return new Tuple2<String, List<Integer>>(t1.f0, res);
                            }
                        },
                        TypeInformation.of(new TypeHint<Tuple2<String, List<Integer>>>() {}));

        //dataStream.asQueryableState
        dataStream.flatMap(new OutMap())
                .keyBy(0)
                .asQueryableState("wordCount", stateDescriptor);

        //输出写入文件中
        dataStream.writeAsText("/Users/rizhiyi/temp/test", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        **/

        /**
         * 从Kafka读取消息并进行初步处理
         */
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        DataStream<String> dataStream1 = env.addSource(
                new FlinkKafkaConsumer<String>(
                        topic,
                        new SimpleStringSchema(),
                        props));
        DataStream<Tuple2<String, Integer>> ds =
                dataStream1.flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        /**
         * 用setQueryableState实现对外暴露状态
         */
        ds.keyBy(0)
                .addSink(new SimpleSink());


        //执行
        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception{
            for(String word : sentence.split(" ")){
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

    public static class OutMap implements FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, List<Integer>>>{

        @Override
        public void flatMap(Tuple2<String, Integer> tuple2, Collector<Tuple2<String, List<Integer>>> collector) throws Exception {
            List<Integer> res = new ArrayList<>();
            res.add(tuple2.f1);
            collector.collect(new Tuple2<>(tuple2.f0, res));
        }
    }

    public static class SimpleSink extends RichSinkFunction<Tuple2<String, Integer>> {

        private ListState<Tuple2<String, Integer>> countListState;
        private LinkedList<Tuple2<String, Integer>> list = new LinkedList<>();
        private static final int MAX = 100;


        private static final String broker_list = "localhost:9092";
        private static final String topic = "wordCount";



        @Override
        public void open(Configuration parameters) throws Exception {


            //创建状态描述符
            ListStateDescriptor<Tuple2<String, Integer>> countListDescriptor =
                    new ListStateDescriptor<Tuple2<String, Integer>>(
                            "countList",
                            TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
            countListDescriptor.setQueryable("wordCount");

            countListState = getRuntimeContext().getListState(countListDescriptor);

            //super.open(parameters);
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            //ListState的更新，即添加列表
            int nowSize = list.size();
            if(nowSize < MAX){
                list.add(value);
            } else{
                list.removeFirst();
                list.addLast(value);
            }
            countListState.update(list);

            Properties props = new Properties();
            props.put("bootstrap.servers", broker_list);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
            String writeValue = value.toString();
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, null, null, writeValue);
            producer.send(record);
        }
    }

    public static class SimpleSchema implements SerializationSchema<Tuple2<String, Integer>>{
        @Override
        public byte[] serialize(Tuple2<String, Integer> tuple2) {
            return tuple2.toString().getBytes();
        }
    }

}
