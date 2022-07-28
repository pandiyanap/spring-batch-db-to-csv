package com.nap.springbatchdbtocsv;

import com.nap.springbatchdbtocsv.model.Employee;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

@SpringBootApplication
@EnableBatchProcessing
public class SpringBatchDbToCsvApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchDbToCsvApplication.class, args);
	}

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	DataSource dataSource;


	@Bean
	public JdbcCursorItemReader<Employee> reader() {
		System.out.println("DataSource test ok");
		JdbcCursorItemReader<Employee> reader = new JdbcCursorItemReader<>();
		reader.setDataSource(dataSource);
		reader.setSql("select id, name, dob, salary, job from mysql.Employee");
		reader.setRowMapper(new RowMapper<Employee>() {
			@Override
			public Employee mapRow(ResultSet rs, int rowNum) throws SQLException {
				Employee employee = new Employee();
				employee.setId(rs.getInt("id"));
				employee.setName(rs.getString("name"));
				employee.setDob(rs.getDate("dob"));
				employee.setSalary(rs.getDouble("salary"));
				employee.setJob(rs.getString("job"));

				return employee;
			}
		});

		return reader;
	}


	@Bean
	public FlatFileItemWriter<Employee> writer() {
		FlatFileItemWriter<Employee> writer = new FlatFileItemWriter<>();
		writer.setResource(new FileSystemResource("/Users/***/NAP/spring-batch-db-to-csv/src/main/java/Employee_Extract.csv"));
		DelimitedLineAggregator<Employee> aggregator = new DelimitedLineAggregator();
		BeanWrapperFieldExtractor<Employee> fieldExtractor = new BeanWrapperFieldExtractor();
		fieldExtractor.setNames(new String[]{"id", "name", "dob", "salary", "job"});
		aggregator.setFieldExtractor(fieldExtractor);
		writer.setLineAggregator(aggregator);
		return writer;

	}

	@Bean
	public Step executeStep() {
		return stepBuilderFactory.get("executeStep").<Employee, Employee>chunk(1).reader(reader())
				.writer(writer()).build();

	}

	@Bean
	public Job processJob() {
		return jobBuilderFactory.get("processJob").incrementer(new RunIdIncrementer()).flow(executeStep()).end().build();
	}


}
