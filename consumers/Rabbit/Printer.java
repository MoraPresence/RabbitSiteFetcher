package consumers.Rabbit;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Scanner;

public class Printer {
    public Scanner cin = new Scanner(System.in);
    public PrintStream pout = new PrintStream(System.out);
    public void printNews(HashMap<String, Document> docVec) {
        for (HashMap.Entry<String, Document> entry : docVec.entrySet()) {
            parseNews(entry.getKey(), entry.getValue());
        }
    }

    public void parseNews(String url, Document doc) {
        Elements spans = doc.select("div [class=article__text__overview]");
        for (Element element : spans) {
            try {
                // pout.println("Author:");
                // pout.println(doc.select("div [class=article__authors__author__name]").get(0).text());
                pout.println("Header:");
                pout.println(doc.select("div [class=article__header__title]").get(0).text());
                pout.println("Authors:");
                pout.println(doc.select("div [class=article__authors]").get(0).text());
                pout.println("Text:");
                pout.println(element.text());
                pout.println("URL:");
                pout.println(url);
            } catch (Exception e) {
                // log.error(e);
            }
        }
    }
}
