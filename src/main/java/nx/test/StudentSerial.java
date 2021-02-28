package nx.test;

import com.alibaba.fastjson.JSON;

import java.io.*;

public class StudentSerial {
    public static void main(String[] args) throws Exception {
        String file = "src/main/data/test.txt";
//对象的序列化

        FileOutputStream fileOutputStream = new FileOutputStream(file);
        Student stu1 = new Student("1221010433", "嘿嘿", 24);
        String s = JSON.toJSONString(stu1);
        System.out.println(s);
        fileOutputStream.write(s.getBytes());
        fileOutputStream.flush();
        fileOutputStream.close();
//以上程序会因为Student没有实现Serializable接口儿抛出异常
//对象的反序列化
//        ObjectInputStream ois = new ObjectInputStream(
//                new FileInputStream(file));
//        Student stu2 = (Student) ois.readObject();
//        System.out.println(stu2);
//        ois.close();
    }
}
