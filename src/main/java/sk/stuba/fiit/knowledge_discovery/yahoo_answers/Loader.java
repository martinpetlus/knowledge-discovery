package sk.stuba.fiit.knowledge_discovery.yahoo_answers;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class Loader {

    public List<Question> load(final String file)
            throws ParserConfigurationException, IOException, SAXException {
        final List<Question> questions = new ArrayList<Question>();

        final File fXmlFile = new File(file);
        final DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        final Document doc = dBuilder.parse(fXmlFile);

        doc.getDocumentElement().normalize();

        final NodeList questionList = doc.getElementsByTagName("vespaadd");

        for (int i = 0; i < questionList.getLength(); i++) {

            final Node question = questionList.item(i).getChildNodes().item(0);

            final NodeList childNodes = question.getChildNodes();

            final Question.Builder builder = new Question.Builder();

            for (int j = 0; j < childNodes.getLength(); j++) {
                Node node = childNodes.item(j);

                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    String nodeName = node.getNodeName();

                    if (nodeName.equals("uri")) {
                        builder.setUri(node.getTextContent());
                    } else if (nodeName.equals("subject")) {
                        builder.setSubject(node.getTextContent());
                    } else if (nodeName.equals("content")) {
                        builder.setContent(node.getTextContent());
                    } else if (nodeName.equals("bestanswer")) {
                        builder.setBestAnswer(node.getTextContent());
                    } else if (nodeName.equals("maincat")) {
                        builder.setMainCat(node.getTextContent());
                    }
                }
            }

            try {
                Question q = builder.build();
                questions.add(q);
            } catch (Exception ex) {
                System.err.println(ex.getMessage());
            }
        }

        return questions;
    }

}