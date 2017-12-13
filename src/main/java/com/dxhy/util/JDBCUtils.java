package com.dxhy.util;

import com.mysql.jdbc.Connection;
import org.slf4j.Logger;

import java.sql.*;
import java.text.ParseException;

import static org.slf4j.LoggerFactory.getLogger;


public class JDBCUtils {

    private static final Logger LOGGER = getLogger(JDBCUtils.class);

    public static void main(String[] args) throws SQLException, ParseException, Exception {
        insert2MapStat("170", "171", "11111111111", "111111", "1111111", "2015-11-13 15:24:06", 11d, 22d, 1);
        insert2MapStat("1100", "171", "11111111111", "111111", "1111111", "2015-11-13 15:24:06", 11d, 22d, 1);
        insert2MapStat("1100", "1100", "11111111111", "111111", "1111111", "2015-11-13 15:24:06", 11d, 22d, 1);
        insert2MapStat("1300", "1301", "11111111111", "111111", "1111111", "2015-11-13 15:24:06", 11d, 22d, 1);
        insert2MapStat("1200", "1201", "11111111111", "111111", "1111111", "2015-11-13 15:24:06", 11d, 22d, 1);
        insert2FPHYProportion("2", 4);
        insert2FPHYProportion("5177", 4);
        insert2FPHYProportion("#", 6);
    }


    private static Connection getConn() {
        String driver = "com.mysql.jdbc.Driver";



        //本地测试
        String url = "jdbc:mysql://10.1.1.13:3306/platform?useUnicode=true&characterEncoding=utf8";
        String username = "root";
        String password = "root";
        Connection conn = null;
        try {
            Class.forName(driver); //classLoader,加载对应驱动
            conn = (Connection) DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void insert2MapStat(String province_Number, String city_Number, String fpdm, String nsrmc, String nsrsbh, String createtime, Double kpje, Double kpse, Integer region_stat) throws SQLException {

        Connection conn = getConn();
        Statement statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        //PreparedStatement stat = conn.prepareStatement(
        //       "insert into test_mapstat(region,city,fpdm,nsrmc,nsrsbh,createtime,kpje,kpse,region_stat) values((select region from region_encoding where code ='" + province_Number + "'), (select region from region_encoding where code ='" + city_Number + "'),'" + fpdm + "', '" + nsrmc + "', '" + nsrsbh + "', '" + createtime + "', " + kpje + ", " + kpse + ", " + region_stat + ");");
        //int i = stat.executeUpdate();
        try {

            int i = statement.executeUpdate(
                    "insert into test_mapstat(region,city,fpdm,nsrmc,nsrsbh,createtime,kpje,kpse,region_stat) values((select region from region_encoding where code ='" + province_Number + "'), (select region from region_encoding where code ='" + city_Number + "'),'" + fpdm + "', '" + nsrmc + "', '" + nsrsbh + "', '" + createtime + "', " + kpje + ", " + kpse + ", " + region_stat + ");");

            LOGGER.info("insert2MapStat 正常插入成功");

            if (i < 1) {
                System.out.println(statement.execute("insert into test_mapstat(region,city,fpdm,nsrmc,nsrsbh,createtime,kpje,kpse,region_stat) values('中国','中国','" + fpdm + "', '" + nsrmc + "', '" + nsrsbh + "', '" + createtime + "', " + kpje + ", " + kpse + ", " + region_stat + ");"));
                LOGGER.info("insert2MapStat 异常插入成功");
            }
        //字段不能为空-异常
        } catch (com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException e) {

            statement.execute("insert into test_mapstat(region,city,fpdm,nsrmc,nsrsbh,createtime,kpje,kpse,region_stat) values('中国','中国','" + fpdm + "', '" + nsrmc + "', '" + nsrsbh + "', '" + createtime + "', " + kpje + ", " + kpse + ", " + region_stat + ");");
            LOGGER.info("insert2MapStat：插入中国");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            conn.close();
            statement.close();
        }

    }

    public static void insert2FPHYProportion(String hydm, int stat) throws SQLException {

        Connection conn = getConn();
        Statement stat1 = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        try {

            int prep = stat1.executeUpdate("insert into test_fphy_prop(fphy_mc,fphy_dm,stat) values((select fphy_mc from fphy_encoding where fphy_dm ='" + hydm + "'),'" + hydm + "'," + stat + ") ");
            LOGGER.info("insert2FPHYProportion：正常插入成功");

            if (prep < 1) {
                System.out.println(stat1.execute("insert into test_fphy_prop(fphy_mc,fphy_dm,stat) values('其他','" + hydm + "'," + stat));
                LOGGER.info("insert2FPHYProportion：插入其他");
            }
        } catch (com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException e) {

            stat1.executeUpdate("insert into test_fphy_prop(fphy_mc,fphy_dm,stat) values('未标注' , '#'," + stat + ")");
            LOGGER.info("insert2FPHYProportion：插入未标注-#");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            conn.close();
        }


    }

    public static void insert2Mysql(String companyname, String updatetime, int count) throws SQLException {

        Connection conn = getConn();
        Statement stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        try {

            int prep = stat.executeUpdate("insert into invoiceCompany(num,updatetime,companyname) select max(num)+" + count + ",'" + updatetime + "','" + companyname + "' from invoiceCompany group by companyname having companyname='" + companyname + "'");
            if (prep < 1) {
                System.out.println(stat.execute("INSERT INTO invoiceCompany (num,updatetime,companyname) values(" + count + ",'" + updatetime + "','" + companyname + "')"));
            }
            System.out.print("插入成功");
        } catch (Exception e) {

            e.printStackTrace();
        } finally {
            conn.close();
        }


    }


    public static void insert2InvoiceCount(String updatetime, int count) throws SQLException {

        Connection conn = getConn();
        Statement stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);


        try {

            int prep = stat.executeUpdate("insert into invoiceCount(num,updatetime) values((select num+" + count + " from (select max(num) as num from invoiceCount) a), '" + updatetime + "')");
            if (prep < 1) {
                System.out.println(stat.execute("insert into invoiceCount(num,updatetime) values (" + count + ",'" + updatetime + "')"));
            }
            System.out.print("插入成功");
        } catch (com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException e) {

            stat.execute("insert into invoiceCount(num,updatetime) values (" + count + ",'" + updatetime + "')");
            System.out.print("插入成功");

        } finally {
            conn.close();
        }


    }


    public static void insert2Region(String dm, String name) throws SQLException {

        Connection conn = getConn();
        Statement stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);


        try {

            int prep = stat.executeUpdate("insert into region_encoding(code, region) values('" + dm + "','" + name + "')");

            System.out.print("插入成功");
        } catch (Exception e) {
            e.printStackTrace();


        } finally {
            conn.close();
        }


    }

    public static void insert2FPHY(String fphy_dm, String fphy_mc) throws SQLException {

        Connection conn = getConn();
        Statement stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        try {

            int prep = stat.executeUpdate("insert into fphy_encoding(fphy_dm, fphy_mc) values('" + fphy_dm + "','" + fphy_mc + "')");

            System.out.print("插入成功");
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            conn.close();
        }


    }


    public static void insert2Mysql3(String companyname, String updatetime, int count) throws SQLException {

        Connection conn = getConn();
        Statement stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);


        try {
//	       int prep = stat.executeUpdate("insert into ic(num,companyname) select max(num)+" + count  + "','" + companyname + " from ic group by companyname having companyname=" + companyname );
//	        if (prep < 1) {
//	          System.out.println(stat.execute("INSERT INTO ic (num,companyname) values(" + count + "','" + companyname + "')"));
//	        }

            int prep = stat.executeUpdate("insert into invoiceCompany(num,updatetime,companyname) select max(num)+" + count + ",'" + updatetime + "','" + companyname + "' from invoiceCompany group by companyname having companyname='" + companyname + "'");
            if (prep < 1) {
                System.out.println(stat.execute("INSERT INTO invoiceCompany (num,updatetime,companyname) values(" + count + ",'" + updatetime + "','" + companyname + "')"));
            }
            System.out.print("插入成功");
        } catch (Exception e) {

            e.printStackTrace();
        } finally {
            conn.close();
        }


    }


    public static void insert2RegionCount(String province_Number, String updatetime, int stat) throws SQLException {

        Connection conn = getConn();
        Statement stat1 = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        try {
//	       int prep = stat.executeUpdate("insert into ic(num,companyname) select max(num)+" + count  + "','" + companyname + " from ic group by companyname having companyname=" + companyname );
//	        if (prep < 1) {
//	          System.out.println(stat.execute("INSERT INTO ic (num,companyname) values(" + count + "','" + companyname + "')"));
//	        }

            int prep = stat1.executeUpdate("insert into region_invoice_count(region,updatetime,stat) values((select region from region_encoding where code ='" + province_Number + "'),'" + updatetime + "'," + stat + ") ");
            if (prep < 1) {
                System.out.println(stat1.execute("insert into region_invoice_count(region,updatetime,stat) values('中国','" + updatetime + "'," + stat + ") "));
            }
            System.out.print("插入成功");
        } catch (Exception e) {

            e.printStackTrace();
        } finally {
            conn.close();
        }


    }


}
