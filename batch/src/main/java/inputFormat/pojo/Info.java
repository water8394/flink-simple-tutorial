package inputFormat.pojo;

public class Info {
    private int id; // 商品id
    private String color; // 商品颜色
    private String country; // 商品国家

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public String toString() {
        return "Info{" +
                "id=" + id +
                ", color='" + color + '\'' +
                ", country='" + country + '\'' +
                '}';
    }
}
