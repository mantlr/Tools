package com.example.audittools;

import lombok.Data;
import org.codehaus.janino.Java;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.util.Scanner;

@SpringBootApplication
@Data
public class AudittoolsApplication {



    public static void main(String[] args) {
        ConfigurableApplicationContext applicationContext = SpringApplication.run(AudittoolsApplication.class, args);
        BlankDome blankDome = (BlankDome) applicationContext.getBean("blankDome");
        try(Scanner sc = new Scanner(System.in)) {
            String[] par=new String[6];
            System.out.println("请输入保险机构代码：" );
            System.out.println("上海 - 000078310000 宁波 - 000078330200800 浙江 - 000078330000  大连 - 000078210200 ");
            par[0]=sc.next();
            System.out.println("请输入保险机构监管系统代码：");
            System.out.println("上海 - 000078310000 宁波 - 000078330200 浙江 - 000078330000  大连 - 000078210200 ");
            par[1]=sc.next();
            System.out.println("请输入采集日期： 格式为：YYYY-MM-DD");
            par[2]=sc.next();
            System.out.println("请输入保险机构名称 如：  sh1 或者 sh2 （主意机构名称简写后加1代表有股份，加2为没有股份）" );
            par[3]=sc.next();
            //String[] par={"000078330200","000078330200","ansheng","2019-12-15"};
            blankDome.readFile(par);
        } catch (IOException e) {
            e.printStackTrace();
        }

        }


    }

