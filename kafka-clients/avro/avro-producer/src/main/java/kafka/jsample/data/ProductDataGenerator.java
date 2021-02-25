package kafka.jsample.data;

import com.github.javafaker.Faker;
import kafka.jsample.models.Category;
import kafka.jsample.models.Product;

import java.util.Locale;

public class ProductDataGenerator {

    static Faker faker = Faker.instance(Locale.JAPAN);

    public static Product getRandomProduct() {
        int categorySelector = faker.random().nextInt(0, Category.values().length - 1);
        Category category;
        String name;
        int price;
        if (Category.Fruites.ordinal() == categorySelector) {
            category = Category.Fruites;
            name = faker.food().fruit();
            price = faker.random().nextInt(10, 1000);
        } else if (Category.Vegetable.ordinal() == categorySelector) {
            category = Category.Vegetable;
            name = faker.food().vegetable();
            price = faker.random().nextInt(10, 500);
        } else if (Category.Spice.ordinal() == categorySelector) {
            category = Category.Spice;
            name = faker.food().spice();
            price = faker.random().nextInt(100, 10000);
        } else {
            category = null;
            name = null;
            price = 0;
        }
        return Product.newBuilder().setId(faker.random().hex()).setCategory(category).setPrice(price).setName(name).build();

    }
}
