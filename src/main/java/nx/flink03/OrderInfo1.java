package nx.flink03;

public class OrderInfo1 {

    private Long orderId;
    private String productName;
    private Double price;

    public OrderInfo1() {

    }

    public OrderInfo1(Long orderId, String productName, Double price) {
        this.orderId = orderId;
        this.productName = productName;
        this.price = price;
    }


    @Override
    public String toString() {
        return "OrderInfo1{" +
                "orderId=" + orderId +
                ", productName='" + productName + '\'' +
                ", price=" + price +
                '}';
    }


    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public static OrderInfo1 string2OrderOnfo1(String line) {
        OrderInfo1 orderInfo1 = new OrderInfo1();
        if (line != null && line.length() > 0) {
            String[] fields = line.split(",");
            orderInfo1.setOrderId(Long.parseLong(fields[0]));
            orderInfo1.setProductName(fields[1]);
            orderInfo1.setPrice(Double.parseDouble(fields[2]));
        }
        return orderInfo1;
    }
}
