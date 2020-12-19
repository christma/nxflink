package nx.etl.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class ETLProcessFunction implements CoFlatMapFunction<String, HashMap<String, String>, String> {
    HashMap<String, String> hmap = new HashMap();

    //{"dt":"2020-12-19 08:59:50","countryCode":"US","data":[{"type":"s3","score":0.3,"level":"D"},{"type":"s1","score":0.3,"level":"D"}]}
    @Override
    public void flatMap1(String line, Collector<String> collector) throws Exception {

        while (true) {
            if (hmap.size() !=0) {
                break;
            }
            System.out.println("redis not have datas or redis errors. ");

        }
        JSONObject jsonObject = JSONObject.parseObject(line);
        String dt = jsonObject.getString("dt");
        String countryCode = jsonObject.getString("countryCode");
        String area = hmap.get(countryCode);
        JSONArray jsonArray = jsonObject.getJSONArray("data");
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject data = jsonArray.getJSONObject(i);
            data.put("dt", dt);
            data.put("area", area);
            System.out.println("大区：" + area);
            collector.collect(data.toJSONString());
        }

    }

    @Override
    public void flatMap2(HashMap<String, String> line, Collector<String> collector) throws Exception {

        System.out.println(line.toString());
        hmap = line;
    }
}
