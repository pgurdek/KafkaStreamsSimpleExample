package kstreams_example.dto;

public class PurchaseCounter {

    private String customerId;
    private int count;

    public PurchaseCounter() {
    }

    public PurchaseCounter(int count) {
        this.count = count;
    }

    public PurchaseCounter(int count, String customerId) {
        this.count = count;
        this.customerId = customerId;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    @Override
    public String toString() {
        return "PurchaseCounter{" +
                "customerId='" + customerId + '\'' +
                ", count=" + count +
                '}';
    }
}
