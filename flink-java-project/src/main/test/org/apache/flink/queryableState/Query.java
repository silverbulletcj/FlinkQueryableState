package org.apache.flink.queryableState;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import sun.jvm.hotspot.runtime.BasicType;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class Query {

    public static void main(String[] args) throws Exception{
        /**
         * proxyHost和proxyProt可以通过Web UI
         * 查看获取你想要访问的state所在的TaskManager的日志获取
         * JobID可以通过查看执行的任务的详情页获取
         */
        String proxyHost = "192.168.0.224";
        int proxyPort = 9069;
        JobID jobID = JobID.fromHexString("9a30ea268d4d856069e17bc8d024a58d");
        String key = "Feb";
        QueryableStateClient client = new QueryableStateClient(proxyHost, proxyPort);
        ListStateDescriptor<Tuple2<String, Integer>> stateDescriptor =
                new ListStateDescriptor<Tuple2<String, Integer>>(
                  "",
                  TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        CompletableFuture<ListState<Tuple2<String, Integer>>> resultFuture =
                client.getKvState(jobID, "wordCount", key, BasicTypeInfo.STRING_TYPE_INFO, stateDescriptor);
        ListState<Tuple2<String, Integer>> res = resultFuture.join();
        Iterator<Tuple2<String, Integer>> iterator = res.get().iterator();
        while(iterator.hasNext()){
            System.out.println(iterator.next().toString());
        }

    }

}
