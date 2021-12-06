public class Event {
    String user_id;
    String user_name;
    int age;

    public Event() {
    }

    public Event(String user_id, String user_name, int age) {
        this.user_id = user_id;
        this.user_name = user_name;
        this.age = age;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user_id='" + user_id + '\'' +
                ", user_name='" + user_name + '\'' +
                ", age=" + age +
                '}';
    }
}
