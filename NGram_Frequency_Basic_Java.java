package project1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

public class NGram_Frequency {
	
	public static HashMap<String, Integer> countNGramFrequency(int n, String text) {
		HashMap<String, Integer> freqCounter = new HashMap<String, Integer>();
		text = text.replaceAll("\\s+", "");
		
		for (int i = 0; i < text.length()-(n-1); i++) {
			String index = text.substring(i, i+n);
			if (!freqCounter.containsKey(index)) {
				freqCounter.put(index, 1);
			} else {
				freqCounter.put(index, freqCounter.get(index)+1);
			}
		}
				
		return freqCounter;
	}

	public static void main(String[] args) throws IOException {
		
		// Read in user input value for n
		Scanner scan = new Scanner(System.in);
		int n = 1;
		boolean acceptable = false;
		
		while (!acceptable) {
			System.out.print("Enter an n-gram length: ");
			try {
				String nStr = scan.nextLine();
				n = Integer.parseInt(nStr);
				acceptable = true;				
			} catch (NumberFormatException e) {
				System.out.println("Invalid value");
			}
		}
			
		try {
			File testFile = new File("./src/project1/test_input.txt");
			String testData = "";
			Scanner fileReader = new Scanner(testFile);
			while (fileReader.hasNextLine()) {
				testData += fileReader.nextLine();
			}
			
			HashMap<String, Integer> freqCounter = countNGramFrequency(n, testData);
			System.out.println(freqCounter);
			
			
		} catch (FileNotFoundException f) {
			f.printStackTrace();
		}
			
			
	}


}
