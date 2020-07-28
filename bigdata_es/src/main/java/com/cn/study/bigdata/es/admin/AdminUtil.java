package com.cn.shool.bigdata.bigdata.es.admin;


import com.cn.shool.bigdata.bigdata.common.file.FileCommon;
import com.cn.shool.bigdata.bigdata.es.client.ESclientUtil;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
public class AdminUtil {
    private static Logger LOG = LoggerFactory.getLogger(AdminUtil.class);

    public static void main(String[] args) throws Exception{
        //创建索引核mapping
       // AdminUtil.createIndices(client,"11111",8,1);
       AdminUtil.buildIndexAndTypes("chl_test8","chl_test8", "test/es/mapping/test.json",15,1);
        //index = 类型+日期
    }

    /**
     * @desc  创建ES  index
     * @param index  索引名
     * @param type   typr
     * @param path   mpping文件路径
     * @param shard  分片树
     * @param replication 副本数
     * @return
     * @throws Exception
     */
    public static boolean buildIndexAndTypes(String index,String type,String path,int shard,int replication) throws Exception{
        boolean flag ;
        //获取客户端
        TransportClient client = ESclientUtil.getClient();
        //读取mapping配置
        String mappingJson = FileCommon.getAbstractPath(path);
        //创建索引
        boolean indices = AdminUtil.createIndices(client, index, shard, replication);

        //如果索引创建陈工
        if(indices){
            LOG.info("创建索引"+ index + "成功");
            //创建mapping
            flag = MappingUtil.addMapping(client, index, type, mappingJson);
        }
        else{
            LOG.error("创建索引"+ index + "失败");
            flag = false;
        }
        return flag;
    }



    /**
     * @desc 判断需要创建的index是否存在
     * */
    public static boolean indexExists(TransportClient client,String index){

        try {
            Thread.currentThread().sleep(15000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        boolean ifExists = false;
        try {
            System.out.println("client===" + client);
            IndicesExistsResponse existsResponse = client.admin().indices().prepareExists(index).execute().actionGet();
            ifExists = existsResponse.isExists();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("判断index是否存在失败...");
            return ifExists;
        }
        return ifExists;
    }


    /**
     * 创建索引
     * @param client
     * @param index
     * @param shard
     * @param replication
     * @return
     */
    public static boolean createIndices(TransportClient client, String index, int shard , int replication){

        if(!indexExists(client,index)) {
            LOG.info("该index不存在，创建...");
            CreateIndexResponse createIndexResponse =null;
            try {
                createIndexResponse = client.admin().indices().prepareCreate(index)
                        .setSettings(Settings.builder()
                                .put("index.number_of_shards", shard)
                                .put("index.number_of_replicas", replication)
                                .put("index.codec", "best_compression")
                                .put("refresh_interval", "30s"))
                        .execute().actionGet();
                return createIndexResponse.isAcknowledged();
            } catch (Exception e) {
                LOG.error(null, e);
                return false;
            }
        }
        LOG.warn("该index " + index + " 已经存在...");
        return false;
    }



}
