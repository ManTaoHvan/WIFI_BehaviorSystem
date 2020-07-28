package com.cn.shool.bigdata.es;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
//作为SpringBoot服务
@SpringBootApplication
//启动注册中心服务
@EnableEurekaClient
//启动feign
@EnableFeignClients
public class EsApplication {

    public static void main(String[] args) {

        SpringApplication.run(EsApplication.class,args);
    }

}
