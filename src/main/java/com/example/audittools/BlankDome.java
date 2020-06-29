package com.example.audittools;


import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Data
@Component("blankDome")
public class BlankDome {
    @Value("${com1}")
    private String com1;

    @Value("${com2}")
    private String com2;

    @Value("${endpath}")
    private String path;

    @Value("${outPath}")
    private String outPathName;

    @Value("${outzip}")
    private String outzipPath;

    @Value("${errLogPath}")
    private String errLogPathName;

    @Value("${ZIPCCLJ}")
    private String zipcclj;

    @Value("${zipPath}")
    private String zipPathName;


    @Value("${CCLJ}")
    private String pathName;

    @Value("${1}")
    private String ningbo;

    @Value("${2}")
    private String shanghai;

    @Value("${3}")
    private String dalian;

    @Value("${4}")
    private String zhejiang;

    @Value("${nb}")
    private String nb;

    //5.数据项中不允许出现ASCII码为0x00-0x1F、0x7F的各类控制符和非打印字符；如出现统一替换为空格（ASCII码0x20）
    private final byte[] controlstr = {0x7F, 0x00, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x22,
            0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F};
    private final String SPLIT = new String(controlstr);

    /**
     * 读入TXT文件
     */


    public void readFile(String[] par) throws IOException {//T_CLAIM_REPORT.txt
        System.out.println("nb==================>" + nb);
        String[] txtStr = new String[47];
        String BXJGJRXKZNO = par[0];
        String BXJGTJDM = par[1];
        String CJRQ = transalateDate(BXJGTJDM, par[2]);
        String BXJGMC = jigou(par[3]);
        String CCLJ = pathName;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd");
        String DATE = simpleDateFormat.format(new Date());


        String[] params = {BXJGJRXKZNO, BXJGTJDM, BXJGMC, CJRQ, CCLJ, DATE};

        File backfile = new File(CCLJ);
        //判断文件是否为空，不为空先清空文件夹
        if(!com1.contains("000078330000")) {
            if (backfile != null && backfile.exists() && backfile.isDirectory()) {
                File[] files = backfile.listFiles();
                if (files != null && files.length > 0) { //此方法判断OK,需要使用数组的长度来判断。
                    for (File file : files) {
                        file.delete();
                    }
                }
            }
        }
        //调用kettle生成文件方法
        KettleUnit.runJob(params, path);


        File file = new File(pathName);
        if (file.exists()) {
            txtStr = file.list();
        }
        for (String txt : txtStr) {
            String txtName;
            String[] tx;
            if (txt.contains("-")) {
                tx = txt.split("-");
                txtName = tx[1];
            } else {
                tx = txt.split(".txt");
                txtName = tx[0];
            }
            String pathname = pathName + "\\" + txt;
            File writeNameLog = null;
            File writeNameLogError = null;
            try {
                File f = new File(outPathName);
                File writeName = new File(outPathName + "\\" + txt);
                if (com1.contains(BXJGTJDM)) {
                    writeNameLog = new File(outPathName + "\\" + txt.replace("txt", "log"));
                    writeNameLog.createNewFile();
                }
                writeNameLogError = new File(errLogPathName + "\\" + txt + "error.log");
                //结束

                writeName.createNewFile();
                writeNameLogError.createNewFile();
                FileReader reader = new FileReader(pathname);
                BufferedReader br = new BufferedReader(reader);
                FileWriter writer = new FileWriter(writeName);

                BufferedWriter out = new BufferedWriter(writer);
                BufferedWriter outLog = null;


                FileWriter writerLogError = new FileWriter(writeNameLogError);
                BufferedWriter outLogError = new BufferedWriter(writerLogError);
                try {
                    String line;
                    int n = 0;
                    //每个TXT文件有多少行数据，即为多少条上报数据
                    //对每一行数据进行处理判断
                    while ((line = br.readLine()) != null) {

                        n++;
                        //替换空字符
                        char[] cl = line.toCharArray();
                        char[] cll = new char[cl.length];
                        //将分隔符先转换成“|”，对每行进行分成数个数据项，为判断数据项是否满足需求判断3,4,5做准备
                        for (int c = 0; c < cl.length; c++) {
                            int m = 0;//每行多少分隔符1.
                            if (cl[c] == 0x7c) {//0x0A为换行符判断
                                System.out.println(0x0A + "%%%");
                                cl[c] = 032;
                            }
                            if (cl[c] == 0x01) {//0x01代表"SOH"
                                m++;
                                cl[c] = 0x7c;//分隔符替换为0x7c代表"|"
                            }

                            //对每行的数据有特殊字符转化为空字符
                            for (int j = 0; j < controlstr.length; j++) {
                                if (controlstr[j] == cl[c]) {
                                    cl[c] = 0x20;//0x20代表(半角)空字符
                                    outLogError.write(txt + "第" + n + "行数据数据项为" + cl + "包含特殊字符\r\n");
                                }
                            }
                            cll[c] = cl[c];
                        }
                        //判断包夹双引号
                        String str = new String(cll);
                        // System.out.println(str);
                        String[] strs = str.split("\\|");
                        String[] strss = new String[strs.length];

                        for (int k = 0; k < strs.length; k++) {
                            char[] ct = strs[0].trim().toCharArray();

                            //判断日期和金额
                            if (txtName.equals("AGT_ACCOUNT_INFO")) {
                                if (k == 4) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        //System.out.println(txt+strs[0]+"第"+n+"行数据数据项为"+strs[k]+"不符合日期格式");
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                                if (k == 0 || k == 1 || k == 2 || k == 3) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项不得为空！\r\n");
                                    }
                                }
                                if (k == 4) {
                                    char[] ckk = strs[k].trim().toCharArray();
                                    char[] cllk = new char[ckk.length];
                                    for (int w = 0; w < ckk.length; w++) {
                                        if (ckk[0] == 0x2B || ckk[ckk.length - 1] == 0x2B || ckk[0] == 0x30 || ckk[ckk.length - 1] == 0x30 || ckk[0] == 0x2E || ckk[ckk.length - 1] == 0x2E) {
                                            System.out.println(txt + "第" + n + "行数据数据项为" + strs[k] + "前或后包含加号、0、句号." + k);
                                        }
                                    }
                                }
                            } else if (txtName.equals("AGT_CODE")) {
                                if (k == 4 || k == 5 || k == 6) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        //System.out.println(txt+strs[0]+"第"+n+"行数据数据项为"+strs[k]+"不符合日期格式");
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                                if (k == 0 || k == 1 || k == 2) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项不得为空！\r\n");
                                    }
                                }
                             /*   if (k == 3) {
                                    if ("".equals(strs[k]) || strs[k].length() != 15) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项不得为空！\r\n");
                                    }
                                }*/

                            }else if (txtName.equals("AGT_CODE_ZJ_ADD")) {
                                    if (k == 4 || k == 5 || k == 6) {
                                        if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                            //System.out.println(txt+strs[0]+"第"+n+"行数据数据项为"+strs[k]+"不符合日期格式");
                                            outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                    "不符合日期格式\r\n");
                                        }
                                    }
                                    if (k == 0 || k == 1 || k == 2|| k == 3) {
                                        if ("".equals(strs[k])) {
                                            outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项不得为空！\r\n");
                                        }
                                    }
                            } else if (txtName.equals("DEPT_INFO")) {
                                if (k == 7 || k == 8) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        //System.out.println(txt+strs[0]+"第"+n+"行数据数据项为"+strs[k]+"不符合日期格式");
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                                if(k == 6){
                                    if("".equals(strs[k]) || strs[k].equals("NA")){
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项"+strs[k]+"有误！\r\n");
                                    }
                                }

                            } else if (txtName.equals("GENERAL_ACCOUNT_INFO")) {
                                if (k == 5 || k == 6) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        //System.out.println(txt+strs[0]+"第"+n+"行数据数据项为"+strs[k]+"不符合日期格式");
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                            } else if (txtName.equals("SALESMAN_INFO")) {
                                if (k == 5 || k == 6) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        //System.out.println(txt+strs[0]+"第"+n+"行数据数据项为"+strs[k]+"不符合日期格式");
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                                if (k == 0 || k == 1 || k == 4 || k == 3 || k == 7) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项不得为空！\r\n");
                                    }
                                }

                            /*    if( k == 2 || k == 3 || k == 4){
                                    if ("9999999".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项为"+ strs[k] +"有误 \r\n");
                                    }
                                }
*/

                            } else if (txtName.equals("SUB_ACCOUNT_CODE_INFO")) {
                                if (k == 7 || k == 8) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        //System.out.println(txt+strs[0]+"第"+n+"行数据数据项为"+strs[k]+"不符合日期格式");
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                            } else if (txtName.equals("VOUCHER_INFO")) {
                                if (k == 0) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        //System.out.println(txt+strs[0]+"第"+n+"行数据数据项为"+strs[k]+"不符合日期格式");
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                                if (k == 7 || k == 8 || k == 9 || k == 10) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                            } else if (txtName.equals("PLAN_INFO")) {
                                if (k == 7) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                            } else if (txtName.equals("T_BILL")) {
                                if (k == 7) {
                                    if (!"".equals(strs[7])) {
                                        if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                            outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] + "不符合日期格式\r\n");
                                        }
                                    }
                                }
                                if (k == 3) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                            } else if (txtName.equals("T_CALI")) {
                                if (k == 1 || k == 2) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        //System.out.println(txt+strs[0]+"第"+n+"行数据数据项为"+strs[k]+"不符合日期格式");
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                                if (k == 17 || k == 18 || k == 19 || k == 20 || k == 21 || k == 22 || k == 23 || k == 24 || k == 25 || k == 26) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                                if (k == 0 || k == 3 || k == 4 || k == 6 || k == 7 || k == 12 || k == 13 || k == 14 || k == 15 || k == 16 ||
                                        k == 17 || k == 18 || k == 19 || k == 20 || k == 21 || k == 22 || k == 23 || k == 24 || k == 25 || k == 26) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项不得为空！\r\n");
                                    }
                                }

                            } else if (txtName.equals("T_CALI_RATE")) {
                                if (k == 3 || k == 4) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                                if (k == 2) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                            } else if (txtName.equals("T_CLAIM")) {
                                if (k == 5 || k == 6 || k == 17 || k == 18 || k == 20 || k == 21 || k == 22 || k == 23 || k == 33 || k == 34) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                                if (k == 1 || k == 14 || k == 19 || k == 24 || k == 25 || k == 27 || k == 29 || k == 30 || k == 31) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }

                                if (k == 0 || k == 1 || k == 2 || k == 4 || k == 5 || k == 6 || k == 7 || k == 9 || k == 14 || k == 16 ||
                                        k == 19 || k == 24 || k == 25 || k == 27 || k == 29 || k == 30 || k == 31 || k == 32 || k == 36 || k == 37 ||
                                        k == 38 || k == 39 || k == 41 || k == 42 || k == 43 || k == 44 || k == 45 || k == 46 || k == 47 || k == 48
                                ) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项不得为空！\r\n");
                                    }
                                }
                            } else if (txtName.equals("T_CLAIM_REPORT")) {
                                if (k == 11) {
                                    if ("".equals(strs[k])) {
                                        strs[k] = "2";
                                        strs[12] = "9999-01-01";
                                        strs[13] = "9999-01-01";
                                    }
                                }
                                if (k == 5 || k == 9 || k == 12 || k == 13) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }

                                if (k == 10) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                                if (k == 0 || k == 2 || k == 4 || k == 6 || k == 10 || k == 11) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项不得为空！\r\n");
                                    }
                                }

                            } else if (txtName.equals("T_CMM_PAY")) {
                                if (k == 10 || k == 2) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        if (k == 10 && "".equals(strs[k])) {
                                            strs[k] = "9999-01-01";
                                        } else {
                                            outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] + "不符合日期格式\r\n");
                                        }
                                    }
                                }
                                if (k == 4 || k == 5) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                            } else if (txtName.equals("T_EDRSMT")) {
                                if (k == 14 || k == 15 || k == 16 || k == 17 || k == 18 || k == 19) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                                if (k == 20 || k == 21 || k == 23 || k == 24 || k == 25 || k == 26 || k == 27 || k == 28 || k == 29 || k == 30 || k == 31 || k == 32 || k == 37 || k == 38) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }

                                if (k == 0 || k == 1 || k == 3 || k == 5 || k == 14 || k == 15 || k == 16 || k == 17 || k == 18 ||
                                        k == 19 || k == 20 || k == 21 || k == 22 || k == 23 || k == 24 || k == 25 || k == 26 || k == 27 || k == 28 ||
                                        k == 29 || k == 30 || k == 31 || k == 32 || k == 33 || k == 37 || k == 35 || k == 38
                                ) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项不得为空！\r\n");
                                    }
                                }

                            } else if (txtName.equals("T_EDRSMT_PAY")) {
                                if (k == 3 || k == 11 || k == 12) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                                if (k == 5 || k == 7 || k == 13) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                                if( k == 10 ){
                                    if ("".equals(strs[k]) || strs[k].equals("9999")) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项不得为空 或为9999！\r\n");
                                    }
                                }
                            } else if (txtName.equals("T_POLICY")) {
                                if (k == 31 || k == 26 || k == 27 || k == 28 || k == 29 || k == 30) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                                if (k == 16 || k == 17 || k == 18 || k == 19 || k == 20 || k == 21 || k == 22 || k == 24 || k == 25 || k == 39 || k == 40) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                                if (k == 0) {
                                    if (strs[0].length() != 20 && strs[0].length() != 17) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项为" + strs[k] + "\r" +
                                                "\n");
                                    }
                                }
                                if (k == 3 || k == 4 || k== 8 || k == 9 || k == 10 || k == 16 || k == 17 || k == 21 || k == 23 || k == 24 || k == 25 || k == 40 || k == 41) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不得为空！\r\n");
                                    }
                                }

                                if (k == 19 || k == 18 || k == 22) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不得为空！\r\n");
                                        strs[k] = "0";
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项为" + "以替换为默认值！\r\n");
                                    }
                                }

                                if (k == 20 || k == 26) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不得为空！\r\n");
                                        strs[k] = "1";
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项为" + "以替换为默认值！\r\n");
                                    }
                                }

                                if (k == 6) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "投保人性质不正确\r\n");
                                    }
                                    if (k == 4 && strs[4].length() < 12) {
                                        if (!"0".equals(strs[6])) {
                                            outLogError.write(txt + "第" + n + "行数据第" + 6 + "个数据项为" + strs[6] +
                                                    "投保人性质不正确\r\n");
                                        }
                                    }
                                    if (k == 4 && strs[4].length() > 12) {
                                        if (!"1".equals(strs[6])) {
                                            outLogError.write(txt + "第" + n + "行数据第" + 6 + "个数据项为" + strs[6] +
                                                    "投保人性质不正确\r\n");
                                        }
                                    }
                                }


                            } else if (txtName.equals("T_POLICY_PAY")) {
                                if (k == 4 || k == 12 || k == 13) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                                if (k == 2 || k == 6 || k == 8 || k == 14 || k == 15) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                                if( k==3 || k == 11){
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项不得为空！\r\n");
                                    }
                                }
                            } else if (txtName.equals("T_BILL_CODE")) {
                                if (k == 3) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                            } else if (txtName.equals("T_REINSURANCE_OUT")) {
                                if (k == 7 || k == 8 || k == 10 || k == 17) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                            } else if (txtName.equals("T_REINSURANCE_IN")) {
                                if (k == 17 || k == 18 || k == 19 || k == 20 || k == 21 || k == 22 || k == 23) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                            } else if (txtName.equals("T_CALI_CLAIM")) {
                                if (k == 1 || k == 5 || k == 6 || k == 7 || k == 8 || k == 9 || k == 10 || k == 11 || k == 12 || k == 13 || k == 14 || k == 15 || k == 16 || k == 17 || k == 18 || k == 19 || k == 20 || k == 21 || k == 22) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项不得为空！\r\n");
                                    }
                                }

                                if (k == 0 || k == 1 || k == 2 || k == 4 || k == 5 || k == 6 || k == 7 || k == 8 || k == 9 || k == 10 || k == 11 || k == 12 || k == 13 || k == 14 || k == 15 || k == 16 || k == 17 || k == 18 || k == 19 || k == 20 || k == 21 || k == 22) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项不得为空！\r\n");
                                    }
                                }

                            } else if (txtName.equals("t_related")) {
                                if (k == 0 || k == 1 || k == 2) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项不得为空！\r\n");
                                    }
                                }
                            } else if (txtName.equals("T_EDRITEM")) {
                                if (k == 5 || k == 6 || k == 7 || k == 8) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                            } else if (txtName.equals("T_PLYVHL")) {
                                if (k == 3) {
                                    if (strs[k].length() != 7 || !strs[k].contains("-")) {
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                                if (k == 13 || k == 14 || k == 15 || k == 17 || k == 18 || k == 19 || k == 20 || k == 21 || k == 22 || k == 23 || k == 24 || k == 25 || k == 26 || k == 27 || k == 28 || k == 29 || k == 30 || k == 31 || k == 32 || k == 33 || k == 34 || k == 35) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                                if (k == 5 || k == 6 || k == 13 || k == 14 || k == 15 || k == 17 || k == 18 || k == 19 || k == 20 || k == 21 || k == 22 || k == 23 || k == 24 || k == 25 || k == 26 || k == 27 || k == 28 || k == 29 || k == 30 || k == 31 || k == 32 || k == 33 || k == 34 || k == 35) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项" + "为空\r\n");
                                    }
                                }

                            } else if (txtName.equals("T_PLYORGPRPT")) {
                                if (k == 5 || k == 6) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }

                                if (k == 0 || k == 1 || k == 2 || k == 3 || k == 4 || k == 5 || k == 6 || k == 7) {
                                    if ("".equals(strs[k])) {
                                        outLogError.write(txt + "\n" + "第" + n + "行数据第" + k + "个数据项" + "为空\r\n");
                                    }
                                }

                            } else if (txtName.equals("T_PLYBATCH")) {
                                if (k == 6 || k == 7 || k == 8) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                            } else if (txtName.equals("T_PLYITEM")) {
                                if (k == 5 || k == 6) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                            } else if (txtName.equals("T_REINSURER_INFO")) {
                                if (k == 2 || k == 4 || k == 5 || k == 6) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                            } else if (txtName.equals("T_INDEMNITY_PAY")) {
                                if (k == 3 || k == 11 || k == 15) {
                                    if (strs[k].length() != 10 || !strs[k].contains("-")) {
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "不符合日期格式\r\n");
                                    }
                                }
                                if (k == 9 || k == 10) {
                                    strs = dateAndNum(txt, n, strs, k, outLogError);
                                }
                                if (k == 8 && !"1".equals(strs[8])) {
                                    if ("".equals(strs[10])) {
                                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                                "领取账号不能为空！！！");
                                    }
                                }
                            }

                            //判断前后空格
                            char[] ckk = strs[k].trim().toCharArray();
                            char[] cllk = new char[ckk.length];
                            for (int w = 0; w < ckk.length; w++) {
                                if (ckk[0] == 0x20 || ckk[ckk.length - 1] == 0x20) {//判断每项数据是否前或后包含空格
                                    outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                            "前或后包含空格\r\n");
                                }

                            }
                            char[] clll = strs[k].trim().toCharArray();
                            char[] clls = new char[clll.length];
                            for (int w = 0; w < clll.length; w++) {
                                if (clll[0] == 0x22 && clll[clll.length - 1] == 0x22) {//判断每项数据是否包含双引号
                                    outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] +
                                            "包含双引号\r\n");
                                    clll[w] = 0x20;
                                }
                                clls[w] = clll[w];
                            }
                            char[] ch = {0x01};
                            if (k == strs.length - 1) {
                                strss[k] = new String(clls);
                            } else {
                                strss[k] = new String(clls) + new String(ch);
                            }
                        }

                        StringBuffer sb = new StringBuffer();
                        for (String sd : strss) {
                            sb.append(sd);
                        }
                        out.write(sb.toString() + "\r\n"); // \r\n即为换行
                        out.flush();

                    }

                    if (com1.contains(BXJGTJDM)) {
                        writeNameLog = new File(outPathName + "\\" + txt.replace("txt", "log"));
                        writeNameLog.createNewFile();
                        FileWriter writerLog = new FileWriter(writeNameLog);
                        outLog = new BufferedWriter(writerLog);
                        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
                        outLog.write(txt + "\r\n" + writeName.length() + "\r\n" + sdf.format(new Date()) + "\r\n" +
                                "Y" + "\r\n" + n + "\r\n");
                        outLog.flush();
                        outLogError.flush();
                    }
                    System.out.println(txt + "总共" + n + "条数据");
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    reader.close();
                    writer.close();
                    if (com1.contains(BXJGTJDM)) {
                        outLog.close();
                    }
                    out.close();
                    outLogError.close();
                    br.close();
                    writerLogError.close();
                }
            } finally {
            }

        }

        //kettle 压缩文件作业的路径 生成文件压缩文
        String mingcheng = "";
        if (BXJGMC.contains("浙江")) {
            mingcheng = "ZJ";
        } else if (BXJGMC.contains("宁波")) {
            mingcheng = "NB";
        } else if (BXJGMC.contains("大连")) {
            mingcheng = "DL";
        } else if (BXJGMC.contains("上海")) {
            mingcheng = "SH";
        }
        String[] zipPar = {mingcheng, CJRQ, zipcclj, DATE, outzipPath};
        KettleUnit.runJob(zipPar, zipPathName);
    }

    private String[] dateAndNum(String txt, int n, String[] strs, int k, BufferedWriter outLogError) {
        char[] ckk = strs[k].trim().toCharArray();
        char[] cllk = new char[ckk.length];
        try {
            if (strs[k].length() > 1 && strs[k].contains(".")) {
                if (ckk[0] == 0x2B || ckk[ckk.length - 1] == 0x2B || ckk[0] == 0x30 || ckk[ckk.length - 1] == 0x30 || ckk[0] == 0x2E || ckk[ckk.length - 1] == 0x2E) {
                    if (ckk[0] == 0x2E) {
                        outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] + "前或后包含加号、0、英文句号" +
                                ".\r\n");
                        strs[k] = "0" + strs[k];
                    }
                }
                //}
            } else if (strs[k].length() > 1 && !strs[k].contains(".")) {
                if (ckk[0] == 0x2B || ckk[ckk.length - 1] == 0x2B || ckk[0] == 0x30 || ckk[0] == 0x2E || ckk[ckk.length - 1] == 0x2E) {
                    outLogError.write(txt + strs[0] + "第" + n + "行数据第" + k + "个数据项为" + strs[k] + "前或后包含加号、0、英文句号.\r\n");
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return strs;
    }

    //对日期进行处理
    private void getDateRule(int n, String[] strs, int k) {
        strs[k] = strs[k].replace(" ", "");
        strs[k] = strs[k].replace("月", "");
        char[] ck = strs[k].trim().toCharArray();
        char[] ckk = new char[ck.length];
        for (int w = 0; w < ck.length; w++) {
            if (ck[w] == 0x2D) {
                ck[w] = 0x7c;//分隔符替换为0x7c代表"|"
            }
            ckk[w] = ck[w];
        }
        strs[k] = new String(ckk);
        System.out.println(strs[k]);
        String[] sk = strs[k].split("\\|");
        System.out.println(n);
        if (sk[1].length() == 1) {
            if (sk[2].equals("99")) {
                strs[k] = "99" + sk[2] + "-0" + sk[1] + "-" + sk[0];
            } else {
                strs[k] = "20" + sk[2] + "-0" + sk[1] + "-" + sk[0];
            }
        } else {
            if (sk[2].equals("99")) {
                strs[k] = "99" + sk[2] + "-" + sk[1] + "-" + sk[0];
            } else {
                strs[k] = "20" + sk[2] + "-" + sk[1] + "-" + sk[0];
            }

        }
    }

    //判断日期格式
    public boolean isDate(String date) {
        /**
         * 判断日期格式和范围
         */
        String rexp = "^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?(" +
                "(0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|" +
                "(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))" +
                "[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))" +
                "[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))";

        Pattern pat = Pattern.compile(rexp);

        Matcher mat = pat.matcher(date);

        boolean dateType = mat.matches();

        return dateType;
    }

    //根据上报机构格式化采集日期
    private String transalateDate(String bxjgdm, String cjrq) {
        String newCjrq = "";
        if (com1.contains(bxjgdm)) {
            newCjrq = cjrq.replace("-", "");
        }
        if (com2.contains(bxjgdm)) {
            newCjrq = cjrq;
        }
        return newCjrq;
    }

    private String jigou(String dm) {
        String jgmc = "";
        if (dm.equals("nb1")) {
            jgmc = "安盛天平财产保险股份有限公司宁波分公司";
        } else if (dm.equals("nb2")) {
            jgmc = "安盛天平财产保险有限公司宁波分公司";
        } else if (dm.equals("sh1")) {
            jgmc = "安盛天平财产保险股份有限公司上海分公司";
        } else if (dm.equals("sh2")) {
            jgmc = "安盛天平财产保险有限公司上海分公司";
        }else if (dm.equals("dl1")) {
            jgmc = "安盛天平财产保险股份有限公司大连分公司";
        }else if (dm.equals("dl2")) {
            jgmc = "安盛天平财产保险有限公司大连分公司";
        }else if (dm.equals("zj1")) {
            jgmc = "安盛天平财产保险股份有限公司浙江分公司";
        }else if (dm.equals("zj2")) {
            jgmc = "安盛天平财产保险有限公司浙江分公司";
        }
        return jgmc;
    }

}
