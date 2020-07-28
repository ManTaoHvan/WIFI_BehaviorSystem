
package com.cn.shool.bigdata.bigdata.es.admin;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MappingUtil {

	private static Logger LOG = LoggerFactory.getLogger(MappingUtil.class);
	//关闭自动添加字段，关闭后索引数据中如果有多余字段不会修改mapping,默认true
	private boolean dynamic = true;

	public static XContentBuilder buildMapping(String tableName) throws IOException {
		XContentBuilder builder = null;
		try {
			builder = XContentFactory.jsonBuilder().startObject()
					.startObject(tableName)
					.startObject("_source").field("enabled", true).endObject()
					.startObject("properties")
					.startObject("id").field("type", "long").endObject()
					.startObject("sn").field("type", "text").endObject()
					.endObject()  
			    .endObject()  
			    .endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return builder;
	}

	public static boolean addMapping(TransportClient client, String index, String type, String jsonString){
		PutMappingResponse putMappingResponse = null;
		try {
			//创建mapping
			PutMappingRequest mappingRequest = new PutMappingRequest(index)
					.type(type).source(JSON.parseObject(jsonString));

			putMappingResponse = client.admin().indices().putMapping(mappingRequest).actionGet();
		} catch (Exception e) {
			LOG.error(null,e);
			e.printStackTrace();
			LOG.error("添加" + type + "的mapping失败....",e);
			return false;
		}
		boolean success = putMappingResponse.isAcknowledged();
		if (success){
			LOG.info("创建" + type + "的mapping成功....");
			return success;
		}
		return success;
	}

	public static void main(String[] args) throws Exception {
		/*String singleConf = ConsulConfigUtil.getSingleConf("es6.1.0/mapping/http");
		int i = singleConf.length() / 2;
		System.out.println(i);*/

	}

}
