package kstreams_example;

import java.util.Date;

public class SecurityDBService {
    public static void saveRecord(Date purchaseDate, String employeeId, String itemPurchased) {
        System.out.println("======================== RECORD SAVED====================");
        System.out.println(purchaseDate.toString());
        System.out.println(employeeId);
        System.out.println(itemPurchased);
        System.out.println("======================== RECORD END ====================");
    }
}
