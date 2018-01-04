package com.example.demo.processor;

import com.example.demo.model.Person;
import iso.std.iso._20022.tech.xsd.pain_007_001.DateAndPlaceOfBirth;
import iso.std.iso._20022.tech.xsd.pain_007_001.ObjectFactory;
import iso.std.iso._20022.tech.xsd.pain_007_001.PersonIdentification5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

/**
 * Created by amarendra on 29/12/17.
 */
public class PersonReverseItemProcessor implements ItemProcessor<Person, PersonIdentification5> {

    private static final Logger log = LoggerFactory.getLogger(PersonReverseItemProcessor.class);

    ObjectFactory objectFactory = new ObjectFactory();

    @Override
    public PersonIdentification5 process(final Person person) throws Exception {
        final PersonIdentification5 personIdentification5 = objectFactory.createPersonIdentification5();
        final DateAndPlaceOfBirth dateAndPlaceOfBirth = objectFactory.createDateAndPlaceOfBirth();
        dateAndPlaceOfBirth.setCityOfBirth(person.getFirstName());
        dateAndPlaceOfBirth.setCityOfBirth(person.getLastName());
        personIdentification5.setDtAndPlcOfBirth(dateAndPlaceOfBirth);
        return personIdentification5;
    }
}
