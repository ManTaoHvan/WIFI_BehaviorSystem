package com.cn.shool.bigdata.bigdata.flume.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.io.File.separator;

/**
 * @author:
 * @description: 文件解析工具类
 * @Date:Created in 2019-10-
 */
public class FlumeFileUtils {


    //实现文件解析，并返回解析内容

    /**
     *
     * @param file      解析的文件
     * @param succPath  一个通用目录
     */
     public static Map<String,Object> parseFile(File file, String succPath) {

         //使用MAP存放解析后的内容
         Map<String,Object> mapResult = new HashMap<>() ;
         //首先需要定义解析成功之后的存储目录
         //动态生成存储目录
         //新的目录
         String fileDirNew = succPath + separator + "2019-10-09";
        //新的文件名 = 新的目录 + 原来文件的文件名
         String fileNameNew = fileDirNew + file.getName();
         try {
             //开始解析
             if(new File(fileNameNew).exists()){
                  //如果此文件已经存在,不做处理
             }else{
                 //如果新目录下不存在此文件，解析
                 List<String> lines = FileUtils.readLines(file);
                 mapResult.put("value",lines);
                 mapResult.put("absolute_filename",fileNameNew);
                 //文件处理完成之后，讲解析完成之后的文件移动到新目录下
                 try {
                     FileUtils.moveToDirectory(file,new File(fileDirNew),true);
                 } catch (IOException e) {
                     System.out.println("移动文件失败");
                 }
             }
         } catch (IOException e) {
             e.printStackTrace();
         }

         return mapResult;
     }


    public static void main(String[] args) throws Exception{
        String rootDir = "F:\\tzjy教学课件\\test\\succ";

        FlumeFileUtils.parseFile(new File("F:\\tzjy教学课件\\test\\data\\1.txt"),rootDir);
    }
}
