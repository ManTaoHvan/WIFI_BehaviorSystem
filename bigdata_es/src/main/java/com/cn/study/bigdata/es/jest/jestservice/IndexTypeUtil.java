package com.cn.shool.bigdata.bigdata.es.jest.jestservice;

import io.searchbox.client.JestClient;


public class IndexTypeUtil {

    public static void main(String[] args) {
        IndexTypeUtil.createIndexAndType("tanslator","es/mapping/tanslator.json");
       // IndexTypeUtil.createIndexAndType("task");
      //  IndexTypeUtil.createIndexAndType("ability");
       // IndexTypeUtil.createIndexAndType("paper");
    }

    public static void createIndexAndType(String index,String jsonPath){
        try{
            JestClient jestClient = JestService.getJestClient();
            JestService.createIndex(jestClient, index);
          //  JestService.createIndexMapping(jestClient,index,index,getSourceFromJson(jsonPath));
        }catch (Exception e){
            e.printStackTrace();
            //LOG.error("创建索引失败",e);
        }
    }
 /*   public static String getSourceFromJson(String path){
        return JsonReader.readJson(path);
    }
*/
    public static String getSource(String index){
        if(index.equals("task")){
            return "{\"_source\": {\n" +
                    "    \"enabled\": true\n" +
                    "  },\n" +
                    "  \"properties\": {\n" +
                    "    \"taskwordcount\": {\n" +
                    "      \"type\": \"long\"\n" +
                    "    },\n" +
                    "    \"taskprice\": {\n" +
                    "      \"type\": \"float\"\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";
        }

        if(index.equals("tanslator")){
            return "{\n" +
                    "  \"_source\": {\n" +
                    "    \"enabled\": true\n" +
                    "  },\n" +
                    "  \"properties\": {\n" +
                    "    \"birthday\": {\n" +
                    "      \"type\": \"text\",\n" +
                    "      \"fields\": {\n" +
                    "        \"keyword\": {\n" +
                    "          \"ignore_above\": 256,\n" +
                    "          \"type\": \"keyword\"\n" +
                    "        }\n" +
                    "      }\n" +
                    "    },\n" +
                    "    \"createtime\":{\n" +
                    "      \"type\": \"long\"\n" +
                    "    },\n" +
                    "    \"updatetime\":{\n" +
                    "      \"type\": \"long\"\n" +
                    "    },\n" +
                    "    \"avgcooperation\":{\n" +
                    "      \"type\": \"long\"\n" +
                    "    },\n" +
                    "    \"cooperationwordcount\":{\n" +
                    "      \"type\": \"long\"\n" +
                    "    },\n" +
                    "    \"cooperation\":{\n" +
                    "      \"type\": \"long\"\n" +
                    "    },\n" +
                    "    \"cooperationtime\":{\n" +
                    "      \"type\": \"long\"\n" +
                    "    },\n" +
                    "    \"age\":{\n" +
                    "      \"type\": \"long\"\n" +
                    "    },\n" +
                    "    \"industry\": {\n" +
                    "      \"type\": \"nested\",\n" +
                    "      \"properties\": {\n" +
                    "        \"industryname\": {\n" +
                    "          \"type\": \"text\",\n" +
                    "          \"fields\": {\n" +
                    "            \"keyword\": {\n" +
                    "              \"ignore_above\": 256,\n" +
                    "              \"type\": \"keyword\"\n" +
                    "            }\n" +
                    "          }\n" +
                    "        },\n" +
                    "        \"count\": {\n" +
                    "          \"type\": \"long\"\n" +
                    "        },\n" +
                    "        \"industryid\": {\n" +
                    "          \"type\": \"text\",\n" +
                    "          \"fields\": {\n" +
                    "            \"keyword\": {\n" +
                    "              \"ignore_above\": 256,\n" +
                    "              \"type\": \"keyword\"\n" +
                    "            }\n" +
                    "          }\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "\n" +
                    "  }\n" +
                    "}";
        }
        return "";
    }

}
