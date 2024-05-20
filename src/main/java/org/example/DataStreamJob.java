/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.Deserializer.JSONDeserializingSchema;
import org.example.dto.SalesPerCategory;
import org.example.dto.SalesPerDay;
import org.example.dto.SalesPerMonth;
import org.example.dto.Transaction;
import org.example.utils.JsonUtils;

import java.sql.Date;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	private static final String jdbcurl = "jdbc:postgresql://localhost:5432/postgres";
	private static final String username = "postgres";
	private static final String password = "postgres";

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String topic = "financial_transaction";
		KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics(topic)
				.setGroupId("flink-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JSONDeserializingSchema())
				.build();

		DataStream<Transaction> transactionDataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

		JdbcExecutionOptions execOptions = new JdbcExecutionOptions.Builder()
				.withBatchSize(1000)
				.withBatchIntervalMs(200)
				.withMaxRetries(5)
				.build();

		JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl(jdbcurl)
				.withDriverName("org.postgresql.Driver")
				.withUsername(username)
				.withPassword(password)
				.build();

		/*create transaction table sink */
		transactionDataStream.addSink(
				JdbcSink.sink(
						"CREATE TABLE IF NOT EXISTS transactions( " +
								"transaction_id VARCHAR(255) PRIMARY KEY, " +
								"product_id VARCHAR(255), " +
								"product_name VARCHAR(255), " +
								"product_category VARCHAR(255), " +
								"product_price DOUBLE PRECISION, " +
								"product_quantity INTEGER, " +
								"product_brand VARCHAR(255), " +
								"total_amount DOUBLE PRECISION, " +
								"currency VARCHAR(255), " +
								"customer_id VARCHAR(255), " +
								"transaction_date TIMESTAMP, " +
								"payment_method VARCHAR(255) " +
								")",
						(JdbcStatementBuilder<Transaction>) (preparedStatment, transaction) ->
						{
						},
						execOptions,
						connectionOptions
				)).name("Table Creation Sink");

		/*sales_per_category table sink */

		transactionDataStream.addSink(
				JdbcSink.sink(
						"CREATE TABLE IF NOT EXISTS sales_per_category( " +
								"transaction_date DATE , " +
								"category VARCHAR(255), " +
								"total_sales DOUBLE PRECISION, " +
								"PRIMARY KEY (transaction_date,category)" +
								")",
						(JdbcStatementBuilder<Transaction>) (preparedStatment, transaction) ->
						{
						},
						execOptions,
						connectionOptions
				)).name("Sales per Category Table Creation Sink");

		/*sales_per_DAY table sink */
		transactionDataStream.addSink(
				JdbcSink.sink(
						"CREATE TABLE IF NOT EXISTS sales_per_day( " +
								"transaction_date DATE, " +
								"total_sales DOUBLE PRECISION, " +
								"PRIMARY KEY (transaction_date)" +
								")",
						(JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) ->
						{
						},
						execOptions,
						connectionOptions
				)).name("Sales per day Table Creation Sink");

		/*sales_per_month table sink */
		transactionDataStream.addSink(
				JdbcSink.sink(
						"CREATE TABLE IF NOT EXISTS sales_per_month( " +
								"year INTEGER, " +
								"month INTEGER, " +
								"total_sales DOUBLE PRECISION, " +
								"PRIMARY KEY (year,month)" +
								")",
						(JdbcStatementBuilder<Transaction>) (preparedStatment, transaction) ->
						{
						},
						execOptions,
						connectionOptions
				)).name("Sales per month Table Creation Sink");



		/* insert transaction data sink */
		transactionDataStream.addSink(
				JdbcSink.sink(
						"INSERT INTO transactions(transaction_id,product_id,product_name,product_category,product_price, " +
								"product_quantity,product_brand,total_amount,currency,customer_id,transaction_date,payment_method ) " +
								"VALUES (?,?,?,?,?,?,?,?,?,?,?,?)" +
								"ON CONFLICT (transaction_id) DO UPDATE SET " +
								"product_id = EXCLUDED.product_id ," +
								"product_name = EXCLUDED.product_name ," +
								"product_category = EXCLUDED.product_category ," +
								"product_price = EXCLUDED.product_price ," +
								"product_quantity = EXCLUDED.product_quantity ," +
								"product_brand = EXCLUDED.product_brand ," +
								"total_amount = EXCLUDED.total_amount ," +
								"currency = EXCLUDED.currency ," +
								"customer_id = EXCLUDED.customer_id ," +
								"transaction_date = EXCLUDED.transaction_date ," +
								"payment_method = EXCLUDED.payment_method ," +
								"transaction_id = EXCLUDED.transaction_id",
						(JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) ->
						{
							preparedStatement.setString(1, transaction.getTransactionId());
							preparedStatement.setString(2, transaction.getProductId());
							preparedStatement.setString(3, transaction.getProductName());
							preparedStatement.setString(4, transaction.getProductCategory());
							preparedStatement.setDouble(5, transaction.getProductPrice());
							preparedStatement.setInt(6, transaction.getProductQuantity());
							preparedStatement.setString(7, transaction.getProductBrand());
							preparedStatement.setDouble(8, transaction.getTotalAmount());
							preparedStatement.setString(9, transaction.getCurrency());
							preparedStatement.setString(10, transaction.getCustomerId());
							preparedStatement.setTimestamp(11, transaction.getTransactionDate());
							preparedStatement.setString(12, transaction.getPaymentMethod());
						},
						execOptions,
						connectionOptions)).name("insert data into transaction sink");


		/* insert into sales per category*/
		transactionDataStream.map(
						transaction -> {

							Date transactionDate = new Date(System.currentTimeMillis());
							String category = transaction.getProductCategory();
							double totalSales = transaction.getTotalAmount();
							return new SalesPerCategory(transactionDate, category, totalSales);

						}
				).keyBy(SalesPerCategory::getCategory)
				.reduce((salesPerCategory, t) ->
						{
							salesPerCategory.setTotalSales(salesPerCategory.getTotalSales() + t.getTotalSales());
							return salesPerCategory;
						}

				).addSink(
						JdbcSink.sink(
								"INSERT INTO sales_per_category(transaction_date,category,total_sales) " +
										"VALUES (?,?,?) " +
										"ON CONFLICT (transaction_date,category) DO UPDATE SET " +
										"total_sales = EXCLUDED.total_sales " +
										"WHERE sales_per_category.category = EXCLUDED.category " +
										"AND sales_per_category.transaction_date = EXCLUDED.transaction_date ",
								(JdbcStatementBuilder<SalesPerCategory>) (preparedStatement, salesPerCategory) ->
								{
									preparedStatement.setDate(1, new Date(System.currentTimeMillis()));
									preparedStatement.setString(2, salesPerCategory.getCategory());
									preparedStatement.setDouble(3, salesPerCategory.getTotalSales());
								},
								execOptions,
								connectionOptions

						)).name("Insert into sales_per_category");


		/* insert into sales per day*/
		transactionDataStream.map(
						transaction -> {

							Date transactionDate = new Date(System.currentTimeMillis());
							double totalSales = transaction.getTotalAmount();
							return new SalesPerDay(transactionDate, totalSales);

						}
				).keyBy(SalesPerDay::getTransactionDate)
				.reduce((salesPerDay, t) ->
						{
							salesPerDay.setTotalSales(salesPerDay.getTotalSales() + t.getTotalSales());
							return salesPerDay;
						}

				).addSink(
						JdbcSink.sink(
								"INSERT INTO sales_per_day(transaction_date,total_sales) " +
										"VALUES (?,?) " +
										"ON CONFLICT (transaction_date) DO UPDATE SET " +
										"total_sales = EXCLUDED.total_sales " +
										"WHERE sales_per_day.transaction_date = EXCLUDED.transaction_date",

								(JdbcStatementBuilder<SalesPerDay>) (preparedStatement, salesPerDay) ->
								{
									preparedStatement.setDate(1, new Date(System.currentTimeMillis()));
									preparedStatement.setDouble(2, salesPerDay.getTotalSales());
								},
								execOptions,
								connectionOptions

						)).name("Insert into sales_per_day");


		/* insert into sales per month*/
		transactionDataStream.map(
						transaction -> {

							Date transactionDate = new Date(System.currentTimeMillis());
							int month = transactionDate.toLocalDate().getMonth().getValue();
							int year = transactionDate.toLocalDate().getYear();
							double totalSales = transaction.getTotalAmount();
							return new SalesPerMonth(year, month, totalSales);

						}
				).keyBy(SalesPerMonth::getMonth)
				.reduce((salesPerMonth, t) ->
						{
							salesPerMonth.setTotalSales(salesPerMonth.getTotalSales() + t.getTotalSales());
							return salesPerMonth;
						}

				).addSink(
						JdbcSink.sink(
								"INSERT INTO sales_per_month(year,month,total_sales) " +
										"VALUES (?,?,?) " +
										"ON CONFLICT (year,month) DO UPDATE SET " +
										"total_sales = EXCLUDED.total_sales " +
										"WHERE sales_per_month.year = EXCLUDED.year " +
										"AND sales_per_month.month = EXCLUDED.month",

								(JdbcStatementBuilder<SalesPerMonth>) (preparedStatement, salesPerMonth) ->
								{
									preparedStatement.setInt(1, salesPerMonth.getYear());
									preparedStatement.setInt(2, salesPerMonth.getMonth());
									preparedStatement.setDouble(3, salesPerMonth.getTotalSales());
								},
								execOptions,
								connectionOptions

						)).name("Insert into sales_per_month");

		/* Elastic Search Sink */
		transactionDataStream.sinkTo(
				new Elasticsearch7SinkBuilder<Transaction>()
						.setHosts(new HttpHost("localhost", 9200, "http"))
						.setEmitter((transaction, runtimeContext, requestIndexer) -> {

							String json = JsonUtils.convertTransactionToJSON(transaction);
							IndexRequest indexRequest = Requests.indexRequest()
									.index("transactions")
									.id(transaction.getTransactionId())
									.source(json, XContentType.JSON);
							requestIndexer.add(indexRequest);
						}).build()).name("Elastic Search Stream");


		transactionDataStream.print();

		env.execute("Flink Streaming");
	}
}
