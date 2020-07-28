package com.cn.shool.bigdata.es;

import io.swagger.annotations.Api;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
@Configuration
@EnableSwagger2
@Api(value = "ES轨迹查询接口", tags = "ES轨迹查询接口")
public class SwaggerConfiguration {
    @Bean
    public Docket createRestApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.cn.shool.bigdata.es.controller"))
                .paths(PathSelectors.any())
                .build();
    }
    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("ES轨迹查询接口")
                .description("swagger-bootstrap-ui")
                .termsOfServiceUrl("wfewf")
               // .contact("developer@mail.com")
                .version("1.0")
                .build();
    }
}
