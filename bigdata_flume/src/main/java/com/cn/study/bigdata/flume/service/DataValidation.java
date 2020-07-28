package com.cn.shool.bigdata.bigdata.flume.service;


import com.cn.shool.bigdata.bigdata.common.regex.Validation;
import com.cn.shool.bigdata.bigdata.flume.constant.ErrorMapFields;
import com.cn.shool.bigdata.bigdata.flume.constant.MapFields;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Description:
 *
 * @author
 * @version 1.0
 * @date 2016/12/10 11:20
 */

public class DataValidation {
    private static final Logger LOG = LoggerFactory.getLogger(DataValidation.class);

  //  private static final TxtConfigurationFileReader reader = TxtConfigurationFileReader.getInstance();
  //  private static final DataTypeConfigurationFileReader datatypereader = DataTypeConfigurationFileReader.getInstance();
  //  private static final ValidationConfigurationFileReader readerValidation = ValidationConfigurationFileReader.getInstance();


    private static Map<String,String>  dataTypeMap;
    private static List<String> listAuthType;
    private static String isErrorES;
    private static final String USERNAME=ErrorMapFields.USERNAME;

    private static final String SJHM=ErrorMapFields.SJHM;
    private static final String SJHM_ERROR=ErrorMapFields.SJHM_ERROR;
    private static final String SJHM_ERRORCODE=ErrorMapFields.SJHM_ERRORCODE;

    private static final String QQ=ErrorMapFields.QQ;
    private static final String QQ_ERROR=ErrorMapFields.QQ_ERROR;
    private static final String QQ_ERRORCODE=ErrorMapFields.QQ_ERRORCODE;

    private static final String IMSI=ErrorMapFields.IMSI;
    private static final String IMSI_ERROR=ErrorMapFields.IMSI_ERROR;
    private static final String IMSI_ERRORCODE=ErrorMapFields.IMSI_ERRORCODE;

    private static final String IMEI=ErrorMapFields.IMEI;
    private static final String IMEI_ERROR=ErrorMapFields.IMEI_ERROR;
    private static final String IMEI_ERRORCODE=ErrorMapFields.IMEI_ERRORCODE;

    private static final String MAC=ErrorMapFields.MAC;
    private static final String CLIENTMAC=ErrorMapFields.CLIENTMAC;
    private static final String STATIONMAC=ErrorMapFields.STATIONMAC;
    private static final String BSSID=ErrorMapFields.BSSID;
    private static final String MAC_ERROR=ErrorMapFields.MAC_ERROR;
    private static final String MAC_ERRORCODE=ErrorMapFields.MAC_ERRORCODE;

    private static final String DEVICENUM=ErrorMapFields.DEVICENUM;
    private static final String DEVICENUM_ERROR=ErrorMapFields.DEVICENUM_ERROR;
    private static final String DEVICENUM_ERRORCODE=ErrorMapFields.DEVICENUM_ERRORCODE;

    private static final String CAPTURETIME=ErrorMapFields.CAPTURETIME;
    private static final String CAPTURETIME_ERROR=ErrorMapFields.CAPTURETIME_ERROR;
    private static final String CAPTURETIME_ERRORCODE=ErrorMapFields.CAPTURETIME_ERRORCODE;


    private static final String EMAIL=ErrorMapFields.EMAIL;
    private static final String EMAIL_ERROR=ErrorMapFields.EMAIL_ERROR;
    private static final String EMAIL_ERRORCODE=ErrorMapFields.EMAIL_ERRORCODE;

    private static final String AUTH_TYPE=ErrorMapFields.AUTH_TYPE;
    private static final String AUTH_TYPE_ERROR=ErrorMapFields.AUTH_TYPE_ERROR;
    private static final String AUTH_TYPE_ERRORCODE=ErrorMapFields.AUTH_TYPE_ERRORCODE;

    private static final String FIRM_CODE=ErrorMapFields.FIRM_CODE;
    private static final String FIRM_CODE_ERROR=ErrorMapFields.FIRM_CODE_ERROR;
    private static final String FIRM_CODE_ERRORCODE=ErrorMapFields.FIRM_CODE_ERRORCODE;

    private static final String STARTTIME=ErrorMapFields.STARTTIME;
    private static final String STARTTIME_ERROR=ErrorMapFields.STARTTIME_ERROR;
    private static final String STARTTIME_ERRORCODE=ErrorMapFields.STARTTIME_ERRORCODE;
    private static final String ENDTIME=ErrorMapFields.ENDTIME;
    private static final String ENDTIME_ERROR=ErrorMapFields.ENDTIME_ERROR;
    private static final String ENDTIME_ERRORCODE=ErrorMapFields.ENDTIME_ERRORCODE;


    private static final String LOGINTIME=ErrorMapFields.LOGINTIME;
    private static final String LOGINTIME_ERROR=ErrorMapFields.LOGINTIME_ERROR;
    private static final String LOGINTIME_ERRORCODE=ErrorMapFields.LOGINTIME_ERRORCODE;
    private static final String LOGOUTTIME=ErrorMapFields.LOGOUTTIME;
    private static final String LOGOUTTIME_ERROR=ErrorMapFields.LOGOUTTIME_ERROR;
    private static final String LOGOUTTIME_ERRORCODE=ErrorMapFields.LOGOUTTIME_ERRORCODE;


    public static Map<String, Object> dataValidation( Map<String, String> map){
        if(map == null){
            return null;
        }
        //errorMap 用来存放错误信息
        Map<String,Object> errorMap = new HashMap<String,Object>();
        //一个字段一个字段校验
        //需要对所有不会对业务产生干扰的数据做校验
        //验证手机号码
        sjhmValidation(map,errorMap);
        //验证MAC
        macValidation(map,errorMap);
        //验证经纬度
        longlaitValidation(map,errorMap);

        //定义自己的清洗规则
        //TODO 大小写统一
        //TODO 时间类型统一
        //TODO 数据字段统一
        //TODO 业务字段转换
        //TODO 数据矫正
      /*  //TODO 验证MAC不能为空
        //TODO 验证IMSI不能为空
        //TODO 验证 QQ IMSI IMEI
        //TODO 验证DEVICENUM是否为空 为空返回错误
        devicenumValidation(map,errorMap);
        //TODO 验证CAPTURETIME是否为空 为空过滤   不为10，14位数字过滤
        capturetimeValidation(map,errorMap);
        //TODO 验证EMAIL
        emailValidation(map,errorMap);
        //TODO 验证STARTTIME ENDTIME LOGINTIME LOGOUTTIME
        timeValidation(map,errorMap);
*/
        return errorMap;
    }


    /**
     * 手机号码验证
     * @param map
     * @param errorMap
     */
    public static void sjhmValidation(Map<String, String> map,Map<String,Object> errorMap){
        if(map.containsKey("phone")){
            String sjhm=map.get("phone");
            //调用正则做手机号码 是否正确的一个 检验
            boolean ismobile = Validation.isMobile(sjhm);
            if(!ismobile){
                //如果不是手机号 搜集错误信息
                errorMap.put(SJHM,sjhm);
                errorMap.put(SJHM_ERROR,SJHM_ERRORCODE);
            }
        }
    }



    //TODO QQ验证  10002  QQ编码 1030001    需要根据DATATYPE来判断数据类型的一起验证
    public static void virtualValidation(String dataType, Map<String, String> map,Map<String,Object> errorMap){

        //TODO USERNAME验证  10023  长度》=2
        if(map.containsKey(ErrorMapFields.USERNAME)){
            String username=map.get(ErrorMapFields.USERNAME);
            if(StringUtils.isNotBlank(username)){
                if(username.length()<2){
                    errorMap.put(ErrorMapFields.USERNAME,username);
                    errorMap.put(ErrorMapFields.USERNAME_ERROR,ErrorMapFields.USERNAME_ERRORCODE);
                }
            }
        }

        //TODO QQ验证  10002  QQ编码 1030001
        if("1030001".equals(dataType)&& map.containsKey(USERNAME)){
            String qqnum= map.get(USERNAME);
            boolean bool = Validation.isQQ(qqnum);
            if(!bool){
                errorMap.put(QQ,qqnum);
                errorMap.put(QQ_ERROR,QQ_ERRORCODE);
            }
        }

        //TODO IMSI验证  10005  IMSI编码 1429997
        if("1429997".equals(dataType)&& map.containsKey(IMSI)){
            String imsi= map.get(IMSI);
            boolean bool = Validation.isIMSI(imsi);
            if(!bool){
                errorMap.put(IMSI,imsi);
                errorMap.put(IMSI_ERROR,IMSI_ERRORCODE);
            }
        }

        //TODO IMEI验证  10006  IMEI编码 1429998
        if("1429998".equals(dataType)&& map.containsKey(IMEI)){
            String imei= map.get(IMEI);
            boolean bool = Validation.isIMEI(imei);
            if(!bool){
                errorMap.put(IMEI,imei);
                errorMap.put(IMEI_ERROR,IMEI_ERRORCODE);
            }
        }
    }


    //MAC验证  10003
    public static void macValidation( Map<String, String> map,Map<String,Object> errorMap){
        if(map == null){
            return ;
        }
        if(map.containsKey("phone_mac")){
            String mac=map.get("phone_mac");
            if(StringUtils.isNotBlank(mac)){
                boolean bool = Validation.isMac(mac);
                if(!bool){
                    LOG.info("MAC验证失败");
                    errorMap.put(MAC,mac);
                    errorMap.put(MAC_ERROR,MAC_ERRORCODE);
                }
            }else{
                    LOG.info("MAC验证失败");
                    errorMap.put(MAC,mac);
                    errorMap.put(MAC_ERROR,MAC_ERRORCODE);
            }
        }
    }




    /**
     * //TODO DEVICENUM验证 为空过滤
     * @param map
     * @param errorMap
     */
    public static void devicenumValidation( Map<String, String> map,Map<String,Object> errorMap){
        if(map == null){
            return ;
        }
        if(map.containsKey("device_number")){
            String devicenum=map.get("device_number");
            if(StringUtils.isBlank(devicenum)){
                errorMap.put(DEVICENUM,"设备编码不能为空");
                errorMap.put(DEVICENUM_ERROR,DEVICENUM_ERRORCODE);
            }
        }
    }

    /**
     *  //TODO CAPTURETIME验证 为空过滤  10019  验证时间长度为10或14位
     * @param map
     * @param errorMap
     */
    public static void capturetimeValidation( Map<String, String> map,Map<String,Object> errorMap){
        if(map == null){
            return ;
        }
        if(map.containsKey(CAPTURETIME)){

            String capturetime=map.get(CAPTURETIME);
            if(StringUtils.isBlank(capturetime)){

                errorMap.put(CAPTURETIME,"CAPTURETIME不能为空");
                errorMap.put(CAPTURETIME_ERROR,CAPTURETIME_ERRORCODE);
            }else{
                boolean bool = Validation.isCAPTURETIME(capturetime);
                if(!bool){
                    errorMap.put(CAPTURETIME,capturetime);
                    errorMap.put(CAPTURETIME_ERROR,CAPTURETIME_ERRORCODE);
                }
            }
        }
    }



    //TODO EMAIL验证 为空过滤 为错误过滤  10004  通过TABLE取USERNAME验证
    public static void emailValidation( Map<String, String> map,Map<String,Object> errorMap){
        if(map == null){
            return ;
        }
        if(map.get("TABLE").equals(EMAIL)){
            String email=map.get(USERNAME);
            if(StringUtils.isNotBlank(email)){
                boolean bool = Validation.isEmail(email);
                if(!bool){
                    errorMap.put(EMAIL,email);
                    errorMap.put(EMAIL_ERROR,EMAIL_ERRORCODE);
                }
            }else{
                errorMap.put(EMAIL,"EMAIL不能为空");
                errorMap.put(EMAIL_ERROR,EMAIL_ERRORCODE);
            }
        }
    }

    //TODO EMAIL验证 为空过滤 为错误过滤  10004  通过TABLE取USERNAME验证
    public static void timeValidation( Map<String, String> map,Map<String,Object> errorMap){
        if(map == null){
            return ;
        }

        if(map.containsKey(STARTTIME)&&map.containsKey(ENDTIME)){

            String starttime=map.get(STARTTIME);
            String endtime=map.get(ENDTIME);
            if(StringUtils.isBlank(starttime)&&StringUtils.isBlank(endtime)){
                errorMap.put(STARTTIME,"STARTTIME和ENDTIME不能同时为空");
                errorMap.put(STARTTIME_ERROR,STARTTIME_ERRORCODE);
                errorMap.put(ENDTIME,"STARTTIME和ENDTIME不能同时为空");
                errorMap.put(ENDTIME_ERROR,ENDTIME_ERRORCODE);
            }else{
                Boolean bool1 = istime(starttime, STARTTIME, STARTTIME_ERROR, STARTTIME_ERRORCODE, errorMap);
                Boolean bool2 = istime(endtime, ENDTIME, ENDTIME_ERROR, ENDTIME_ERRORCODE, errorMap);

                if(bool1&&bool2&&(starttime.length()!=endtime.length())){
                    errorMap.put(STARTTIME,"STARTTIME和ENDTIME长度不等 STARTTIME："+starttime + "\t"+"ENDTIME:" + endtime);
                    errorMap.put(STARTTIME_ERROR,STARTTIME_ERRORCODE);
                    errorMap.put(ENDTIME,"STARTTIME和ENDTIME长度不等 STARTTIME："+starttime + "\t"+"ENDTIME:" + endtime);
                    errorMap.put(ENDTIME_ERROR,ENDTIME_ERRORCODE);
                }
                else if(bool1&&bool2&&(endtime.compareTo(starttime)<0)){
                    errorMap.put(STARTTIME,"ENDTIME必须大于STARTTIME  STARTTIME:"+starttime + "\t"+"ENDTIME:" + endtime);
                    errorMap.put(STARTTIME_ERROR,STARTTIME_ERRORCODE);
                    errorMap.put(ENDTIME,"ENDTIME必须大于STARTTIME  STARTTIME:"+starttime + "\t"+"ENDTIME:" + endtime);
                    errorMap.put(ENDTIME_ERROR,ENDTIME_ERRORCODE);
                }
            }

        }else if(map.containsKey(LOGINTIME)&&map.containsKey(LOGOUTTIME)){

            String logintime=map.get(LOGINTIME);
            String logouttime=map.get(LOGOUTTIME);

            if(StringUtils.isBlank(logintime)&&StringUtils.isBlank(logouttime)){
                errorMap.put(LOGINTIME,"LOGINTIME和LOGOUTTIME不能同时为空");
                errorMap.put(LOGINTIME_ERROR,LOGINTIME_ERRORCODE);
                errorMap.put(LOGOUTTIME,"LOGINTIME和LOGOUTTIME不能同时为空");
                errorMap.put(LOGOUTTIME_ERROR,LOGOUTTIME_ERRORCODE);
            }else{

                Boolean bool1 = istime(logintime, LOGINTIME, LOGINTIME_ERROR, LOGINTIME_ERRORCODE, errorMap);
                Boolean bool2 = istime(logouttime, LOGOUTTIME, LOGOUTTIME_ERROR, LOGOUTTIME_ERRORCODE, errorMap);

                if(bool1&&bool2&&(logintime.length()!=logouttime.length())){
                    errorMap.put(LOGINTIME,"LOGOUTTIME LOGINTIME长度不等 LOGINTIME："+logintime + "\t"+"LOGOUTTIME:" + logouttime);
                    errorMap.put(LOGINTIME_ERROR,LOGINTIME_ERRORCODE);
                    errorMap.put(LOGOUTTIME,"LOGOUTTIME LOGINTIME长度不等 LOGINTIME："+logintime + "\t"+"LOGOUTTIME:" + logouttime);
                    errorMap.put(LOGOUTTIME_ERROR,LOGOUTTIME_ERRORCODE);
                }
                else if(bool1&&bool2&&(logouttime.compareTo(logintime)<0)){
                    errorMap.put(LOGINTIME,"LOGOUTTIME必须大于LOGINTIME  LOGINTIME:"+logintime + "\t"+"LOGOUTTIME:" + logouttime);
                    errorMap.put(LOGINTIME_ERROR,LOGINTIME_ERRORCODE);
                    errorMap.put(LOGOUTTIME,"LOGOUTTIME必须大于LOGINTIME  LOGINTIME:"+logintime + "\t"+"LOGOUTTIME:" + logouttime);
                    errorMap.put(LOGOUTTIME_ERROR,LOGOUTTIME_ERRORCODE);
                }
            }

        }
    }



    //TODO AUTH_TYPE验证  为空过滤 为错误过滤  10020
    public static void authtypeValidation( Map<String, String> map,Map<String,Object> errorMap){
        if(map == null){
            return ;
        }
        String fileName=map.get(MapFields.FILENAME);

        if(fileName.split("_").length<=2){
            map = null;
            return;
        }

        if(StringUtils.isNotBlank(fileName)){
            if("bh".equals(fileName.split("_")[2])||"wy".equals(fileName.split("_")[2])||"yc".equals(fileName.split("_")[2])){

                return ;
            }else if(map.containsKey(AUTH_TYPE)){

                String authtype=map.get(AUTH_TYPE);
                if(StringUtils.isNotBlank(authtype)){

                    if(listAuthType.contains(authtype)){

                        if("1020004".equals(authtype)){
                            String sjhm=map.get(MapFields.AUTH_ACCOUNT);

                            boolean ismobile = Validation.isMobile(sjhm);
                            if(!ismobile){
                                errorMap.put(SJHM,sjhm);
                                errorMap.put(SJHM_ERROR,SJHM_ERRORCODE);
                            }
                        }
                        if("1020002".equals(authtype)){

                            String mac=map.get(MapFields.AUTH_ACCOUNT);
                            boolean ismac = Validation.isMac(mac);
                            if(!ismac){
                                errorMap.put(MAC,mac);
                                errorMap.put(MAC_ERROR,MAC_ERRORCODE);
                            }
                        }

                    }else{
                        errorMap.put(AUTH_TYPE,"AUTHTYPE_LIST 影射里没有"+ "\t"+ "["+ authtype+"]");
                        errorMap.put(AUTH_TYPE_ERROR,AUTH_TYPE_ERRORCODE);
                    }
                }else{
                    errorMap.put(AUTH_TYPE,"AUTH_TYPE 不能为空");
                    errorMap.put(AUTH_TYPE_ERROR,AUTH_TYPE_ERRORCODE);
                }
            }
        }
    }




    private static final String LONGITUDE = "longitude";
    private static final String LATITUDE = "latitude";
    private static final String LONGITUDE_ERROR=ErrorMapFields.LONGITUDE_ERROR;
    private static final String LONGITUDE_ERRORCODE=ErrorMapFields.LONGITUDE_ERRORCODE;
    private static final String LATITUDE_ERROR=ErrorMapFields.LATITUDE_ERROR;
    private static final String LATITUDE_ERRORCODE=ErrorMapFields.LATITUDE_ERRORCODE;
    /**
     * 经纬度验证  错误过滤  10012  10013
     * @param map
     * @param errorMap
     */
    public static void longlaitValidation( Map<String, String> map,Map<String,Object> errorMap){
        if(map == null){
            return ;
        }
        if(map.containsKey(LONGITUDE)&&map.containsKey(LATITUDE)){
            String longitude=map.get(LONGITUDE);
            String latitude=map.get(LATITUDE);

            boolean bool1 = Validation.isLONGITUDE(longitude);
            boolean bool2 = Validation.isLATITUDE(latitude);

            if(!bool1){
                errorMap.put(LONGITUDE,longitude);
                errorMap.put(LONGITUDE_ERROR,LONGITUDE_ERRORCODE);
            }
            if(!bool2){
                errorMap.put(LATITUDE,latitude);
                errorMap.put(LATITUDE_ERROR,LATITUDE_ERRORCODE);
            }
        }
    }





    public static Boolean istime(String time,String str1,String str2,String str3,Map<String,Object> errorMap){

        if(StringUtils.isNotBlank(time)){
            boolean bool = Validation.isCAPTURETIME(time);
            if(!bool){
                errorMap.put(str1,time);
                errorMap.put(str2,str3);
                return false;
            }
            return true;
        }
        return false;
    }
}
