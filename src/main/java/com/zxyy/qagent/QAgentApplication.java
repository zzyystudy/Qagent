package com.zxyy.qagent;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@MapperScan({
        "com.zxyy.qagent.rag.dao.mapper",
        "com.zxyy.qagent.knowledge.dao.mapper"
})
@SpringBootApplication
public class QAgentApplication {
    public static void main(String[] args) {
        SpringApplication.run(QAgentApplication.class);
    }
}
