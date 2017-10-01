package com.verizon.extracctor;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

public class BasicWebCrawler {
	List<WebElement> review = null, pubDate = null, reviewTitle = null, userNickName = null;
	public BasicWebCrawler() {
		review = new ArrayList<WebElement>();
		pubDate = new ArrayList<WebElement>();
		reviewTitle = new ArrayList<WebElement>();
		userNickName = new ArrayList<WebElement>();
	}
	public void getPageLinks(String URL) throws InterruptedException, IOException {

		System.setProperty("webdriver.gecko.driver", "/home/ashutoshskhandelwal/Desktop/eclipse/geckodriver");
		WebDriver driver = new FirefoxDriver();
		driver.get(URL);
		Thread.sleep(500);
		String deviceBrandName = (driver.findElement(By.xpath("//*[@id='device-name-wrapper']/h1/span[1]")).getText());
		String deviceName = (driver.findElement(By.xpath("//*[@id='device-name-wrapper']/h1/span[2]")).getText());
		String device = deviceBrandName + " " + deviceName;
		int reviewCount = Integer.parseInt((driver
				.findElement(By.xpath("//*[@id='BVRRSearchContainer']/div/div/div/div/div/div[1]/div/dl/dd[3]/span/a"))
				.getText()).split(" ")[0]);
		if (reviewCount >= 400) {
			while (true) {
				review.addAll(driver.findElements(By.xpath(
						"//*[@id='BVRRContainer']/div/div/div/div/ol/li[*]/div/div/div/div[2]/div/div/div[1]/p")));
				pubDate.addAll(driver.findElements(By.xpath(
						"//*[@id='BVRRContainer']/div/div/div/div/ol/li[*]/div/div[1]/div/div[1]/div/div[1]/div/div/div/div/meta[2]")));
				reviewTitle.addAll(driver.findElements(By.xpath(
						"//*[@id='BVRRContainer']/div/div/div/div/ol/li[*]/div/div[1]/div/div[1]/div/div[2]/h4")));
				userNickName.addAll(driver.findElements(By.xpath(
						"//*[@id='BVRRContainer']/div/div/div/div/ol/li[*]/div/div[1]/div/div[1]/div/div[1]/div/div/div/h3")));
				WebElement next = driver.findElement(
						By.xpath("//*[@id='BVRRContainer']/div/div/div/div/div[3]/div/ul/li[2]/a/span[2]"));
				Thread.sleep(5);
				((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView(true);", next);
				Thread.sleep(500);
				if (review.size() >= 400) {
					break;
				}
			}
		} else
			System.out.println("Review count is less than 4000");

		final String COMMA_DELIMITER = "<";//used this as a delimiter, as few reviews had ",";
		final String NEW_LINE = "\n";
		final String FILE_HEADER = "Device,Title,ReviewText,SubmissionTime,UserNickName";
		FileWriter fileWriter = null;
		String fileName = System.getProperty("user.home") + "/Scrapped_"
				+ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toString() + ".csv";
		File f = new File(fileName);
		if (f.exists() && !f.isDirectory()) {
			f.delete();
		}
		try {
			fileWriter = new FileWriter(fileName);
			fileWriter.append(FILE_HEADER.toString());
			fileWriter.append(NEW_LINE);
			for (int i = 0; i < review.size(); i++) {
				fileWriter.append(device);
				fileWriter.append(COMMA_DELIMITER);
				fileWriter.append(reviewTitle.get(i).getText());
				fileWriter.append(COMMA_DELIMITER);
				fileWriter.append(review.get(i).getText());
				fileWriter.append(COMMA_DELIMITER);
				fileWriter.append(pubDate.get(i).getAttribute("content"));
				fileWriter.append(COMMA_DELIMITER);
				fileWriter.append(userNickName.get(i).getText());
				fileWriter.append(NEW_LINE);
			}
		} catch (Exception e) {
			System.out.println("Error creating csv file!");
			e.printStackTrace();
		} finally {
			try {
				fileWriter.flush();
				fileWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		driver.quit();
		String hdfsPath = "hdfs://localhost:54310/VerizonScrapper/Scrapped_"
				+ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toString() + ".csv";
		InputStream in = new BufferedInputStream(new FileInputStream(fileName));
		Configuration cf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), cf);
		OutputStream os = fs.create(new Path(hdfsPath));
		IOUtils.copyBytes(in, os, 4096, true);
	}

	public static void main(String[] args) throws InterruptedException, IOException {
		new BasicWebCrawler().getPageLinks("https://www.verizonwireless.com/smartphones/samsung-galaxy-s7/");

	}

}
