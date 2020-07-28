package com.cn.shool.bigdata.bigdata.spark.warn.dao;

import com.cn.shool.bigdata.bigdata.common.db.DBCommon;
import com.cn.shool.bigdata.bigdata.spark.warn.domain.WarningMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
public class WarningMessageDao {

    private static final Logger LOG = LoggerFactory.getLogger(WarningMessageDao.class);

    /**
     * 写入消息到mysql
     * @param warningMessage
     * @return
     */
    public static Integer insertWarningMessageReturnId(WarningMessage warningMessage) {
        Connection conn= DBCommon.getConn("test");
        String sql="insert into warn_message(alarmruleid,sendtype,senfinfo,hittime,sendmobile,alarmtype) " +
                "values(?,?,?,?,?,?)";

        PreparedStatement stmt=null;
        ResultSet resultSet=null;
        int id=-1;
        try{
            stmt = conn.prepareStatement(sql);
            stmt.setString(1,warningMessage.getAlarmRuleid());
            stmt.setInt(2,Integer.valueOf(warningMessage.getSendType()));
            stmt.setString(3,warningMessage.getSenfInfo());
            stmt.setTimestamp(4,new Timestamp(System.currentTimeMillis()));
            stmt.setString(5,warningMessage.getSendMobile());
            stmt.setInt(6,Integer.valueOf(warningMessage.getAlarmType()));
            id = stmt.executeUpdate();
        }catch(Exception e) {
            LOG.error(null,e);
        }finally {
            try {
                DBCommon.close(conn,stmt,resultSet);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return id;
    }
}
