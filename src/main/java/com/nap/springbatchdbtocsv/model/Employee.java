package com.nap.springbatchdbtocsv.model;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Getter
@Setter
@Data
public class Employee {
    int id;
    String name;
    Date dob;
    Double salary;
    String job;
}
