package com.cn.shool.bigdata.bigdata.flume.source;

import com.cn.shool.bigdata.bigdata.flume.constant.ConstantFields;
import com.cn.shool.bigdata.bigdata.flume.utils.FlumeFileUtils;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.*;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
public class FolderSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger logger = Logger.getLogger(FolderSource.class);

    private int filenum;  //每批处理文件数
    private String dirs;   //flume监控德FTP文件目录
    private Collection<File> allFiles; //监控目录下的总文件数
    private List<File> listFile;       //每批次处理的文件数
    private String successfile;         // 文件处理成功写入的目录；
    private List<Event> eventList;

    //配置文件读取
    @Override
    public void configure(Context context) {
        filenum = context.getInteger("filenum");
        dirs = context.getString("dirs");
        successfile = context.getString("successfile");
        eventList = new ArrayList<>();
    }

    //业务功能实现
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        try {
            Thread.currentThread().sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            //进行文件读取
            allFiles = FileUtils.listFiles(new File(dirs), new String[]{"txt"}, true);
            //小批次处理，如果目录下文件过多，可能导致
            int fileCount = allFiles.size();
            //文件截取 如果文件数大于3个，就只截取2个进行处理
            if(fileCount >= filenum){
                listFile = ((List<File>) allFiles).subList(0, filenum);
            }else{
                listFile = ((List<File>) allFiles);
            }
            //每批次的文件数拿到了，下面是进行解析
            //遍历文件集合进行解析
            if(listFile.size()>0){
                listFile.forEach(file->{
                    //获取文件名
                    String fileName = file.getName();
                    //TODO 文件解析
                    // 解析文件的过程中，需要实现文件备份，异常文件处理
                    //文件解析之后返回2个值，一个是value  一个是absolute_filename
                    Map<String, Object> stringObjectMap = FlumeFileUtils.parseFile(file, successfile);
                    String absolute_filename = stringObjectMap.get(ConstantFields.ABSOLUTE_FILENAME).toString();
                    List<String> listLines = (List<String>)stringObjectMap.get(ConstantFields.VALUE);
                    //stringObjectMap.get("absolute_filename");
                    //TODO 批处理
                    listLines.forEach(line->{
                        Event e = new SimpleEvent();
                        e.setBody(line.getBytes());
                        Map<String,String> headers = new HashMap<>();
                        headers.put(ConstantFields.FILENAME,fileName);
                        headers.put(ConstantFields.ABSOLUTE_FILENAME,absolute_filename);
                        e.setHeaders(headers);
                        eventList.add(e);
                    });

                    //数量控制  时间控制
                    if(eventList.size() >= 2){
                        getChannelProcessor().processEventBatch(eventList);
                        logger.info("批量推送数据到channel" + eventList + "成功");
                        eventList.clear();
                    }
                });
            }
            status = Status.READY;
        } catch (Exception e) {
            status = Status.BACKOFF;
            logger.error(null,e);
        }
        return status;
    }
}
