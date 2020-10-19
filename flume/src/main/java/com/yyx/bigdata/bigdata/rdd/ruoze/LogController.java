package com.yyx.bigdata.bigdata.rdd.ruoze;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author PK哥
 **/
@Controller
public class LogController {

    private static final Logger logger = Logger.getLogger("LogController");

    @PostMapping(value = "/upload")
    @ResponseBody
    public void upload(@RequestBody String log) {
        System.out.println(log);
        logger.info(log);
    }
}
