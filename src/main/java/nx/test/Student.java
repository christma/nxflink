package nx.test;

import java.io.Serializable;

public class Student implements Serializable {
    private String Number;
    private String Name;
    private int age;

    public Student(String number, String name, int age) {
        Number = number;
        Name = name;
        this.age = age;
    }

    public String getNumber() {
        return Number;
    }

    public void setNumber(String number) {
        Number = number;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "Number='" + Number + '\'' +
                ", Name='" + Name + '\'' +
                ", age=" + age +
                '}';
    }
}
