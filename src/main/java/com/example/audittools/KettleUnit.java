package com.example.audittools;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

/**
 * @auther BoHanMa
 * @date 2019/10/12 9:47
 */
public class KettleUnit {

    /**
     * java 调用 kettle 的job
     */
    public static void runJob(String[] params, String jobPath) {
        try {
            KettleEnvironment.init();
            // jobname 是Job脚本的路径及名称
            JobMeta jobMeta = new JobMeta(jobPath, null);
            Job job = new Job(null, jobMeta);
            // 向Job 脚本传递参数，脚本中获取参数值：${参数名}
            job.setArguments(params);
            job.start();
            job.waitUntilFinished();
            if (job.getErrors() > 0) {
                throw new Exception(
                        "There are errors during job exception!(执行job发生异常)");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void zipJob(String[] params,String jobPath){
        try {
            KettleEnvironment.init();
            // jobname 是Job脚本的路径及名称
            JobMeta jobMeta = new JobMeta(jobPath, null);
            Job job = new Job(null, jobMeta);
            // 向Job 脚本传递参数，脚本中获取参数值：${参数名}
            job.setArguments(params);
//            System.out.println(job);
            job.start();
            job.waitUntilFinished();
            if (job.getErrors() > 0) {
                throw new Exception(
                        "There are errors during job exception!(执行job发生异常)");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
