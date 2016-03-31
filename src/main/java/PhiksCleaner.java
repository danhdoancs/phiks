import org.w3c.dom.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import javax.xml.stream.*;
import javax.xml.stream.events.XMLEvent;
import javax.xml.stream.events.StartElement;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PhiksCleaner {
	public static void main(String[] args) throws IOException {
		List<String> featureList = new ArrayList();
		// Prepare output cleaned dataset file
		File cleanedDatasetFile = new File("datasets/cleaned_wiki_articles.txt");
		BufferedWriter bw = new BufferedWriter(new FileWriter(cleanedDatasetFile));
	
		// Prepare output feature list file
		File featureFile = new File("datasets/feature_list.txt");
		BufferedWriter bw2 = new BufferedWriter(new FileWriter(featureFile));

		// Read dictionary
		File dict = new File("database/american-english");
		BufferedReader br = new BufferedReader(new FileReader(dict));
		String line;
		List<String> dictList = new ArrayList(); 
		while((line = br.readLine()) != null) {
			//System.out.println(line);
			dictList.add(line);
		}
		br.close();
	 
	 	// Read stopWord list
	 	File stopWordFile = new File("database/stopwordslist.txt");
	 	br = new BufferedReader(new FileReader(stopWordFile));
	 	List<String> stopwordList = new ArrayList();
	 	while((line = br.readLine()) != null) {
			stopwordList.add(line);
	 	}
		br.close();

		// Read the wiki dataset
		try {
		//	System.out.println(System.getProperty("user.dir"));
			String xmlFile = "datasets/wiki_articles_small.xml";
			FileInputStream fileInputStream = new FileInputStream(xmlFile);
			XMLInputFactory xmlFactory = XMLInputFactory.newInstance();
			XMLEventReader xmlEventReader = xmlFactory.createXMLEventReader(fileInputStream);
			while (xmlEventReader.hasNext()) {
				XMLEvent xmlEvent = xmlEventReader.nextEvent();
				if(xmlEvent.isStartElement()) {
					StartElement startElement = xmlEvent.asStartElement();
					if (startElement.getName().getLocalPart().equals("text")) {
						xmlEvent = xmlEventReader.nextEvent();
						//System.out.println("Page: " + xmlEvent.asCharacters().getData().toString());
						String pageText = xmlEvent.asCharacters().getData().toString();
						pageText = pageText.replaceAll("[^a-zA-Z0-9\\s]", "");
						pageText = pageText.replaceAll("\\n+", " ");
						pageText = pageText.trim().replaceAll(" +", " ");
						pageText = pageText.toLowerCase();

						// Conver to string list
						List<String> wordList = Arrays.asList(pageText.split(" "));
						// Remove non-meaning words and stopwords
						List<String> cleanedWordList = new ArrayList();
						String cleanedLine = "";
						int wordCount = 0;
						for (String word : wordList) {
							if(dictList.contains(word) && !stopwordList.contains(word)) {
								// Check and add into feature list
								if (!featureList.contains(word)) {
									featureList.add(word);
									// Write new feature into feature list file
									bw2.write(word);
									bw2.newLine();
								}
								// Check for duplication
								if (!cleanedWordList.contains(word)) {
									cleanedWordList.add(word);
									cleanedLine += word + " ";
									wordCount++;
								}
							}  
								//System.out.println(word);
						}
						// Write into cleaned dataset 01
						// Skipp all pages with less than 3 words
						if (cleanedLine != "" && wordCount > 2) {
							bw.write(cleanedLine);
							bw.newLine();
						}
					}
				}
			}
		
			bw.flush();
			bw.close();
			bw2.flush();
			bw2.close();

			// Create final dataset
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
