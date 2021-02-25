package kafka.jsample.data;

import com.github.javafaker.Faker;
import kafka.jsample.models.Address;
import kafka.jsample.models.Phone;
import kafka.jsample.models.User;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class UserDataGenerator {

    static Faker faker = Faker.instance(Locale.JAPAN);

    public static User getRandomUser() {
        Address address = Address.newBuilder()
                                 .setBuildingNo(faker.address().buildingNumber())
                                 .setCity(faker.address().city())
                                 .setZipCode(faker.address().zipCode())
                                 .setCountry(Locale.JAPAN.getDisplayCountry())
                                 .build();
        Phone phone = Phone.newBuilder().setNumber(faker.phoneNumber().cellPhone()).build();
        int noOfEmailAddress = faker.random().nextInt(1, 5);
        List<CharSequence> emailAddresses = IntStream.range(0, noOfEmailAddress)
                                                     .mapToObj(n -> faker.internet().emailAddress())
                                                     .collect(Collectors.toList());
        return User.newBuilder()
                   .setId(faker.random().hex())
                   .setAddress(address)
                   .setPhone(phone)
                   .setEmailId(emailAddresses)
                   .setFirstName(faker.name().firstName())
                   .setLastName(faker.name().lastName())
                   .build();
    }
}