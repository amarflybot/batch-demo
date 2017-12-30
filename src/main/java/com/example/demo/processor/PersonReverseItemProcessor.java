package com.example.demo.processor;

import com.example.demo.model.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

/**
 * Created by amarendra on 29/12/17.
 */
public class PersonReverseItemProcessor implements ItemProcessor<Person, Person> {

    private static final Logger log = LoggerFactory.getLogger(PersonReverseItemProcessor.class);

    @Override
    public Person process(final Person person) throws Exception {
        final String firstName = person.getFirstName().toLowerCase();
        final String lastName = person.getLastName().toLowerCase();
        final Integer id = person.getId();

        final Person transformedPerson = new Person(firstName, lastName, id);

        //log.info("Converting (" + person + ") into (" + transformedPerson + ")");

        return transformedPerson;
    }
}
