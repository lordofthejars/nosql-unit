package com.lordofthejars.nosqlunit.marklogic.content;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static com.lordofthejars.nosqlunit.marklogic.content.XmlContent.ATTR_COLLECTIONS;
import static com.lordofthejars.nosqlunit.marklogic.content.XmlContent.ATTR_ID;
import static java.util.stream.IntStream.range;

public class XmlParser {

    private DocumentBuilderFactory documentBuilderFactory;

    private TransformerFactory transformerFactory;

    public XmlParser(DocumentBuilderFactory documentBuilderFactory, TransformerFactory transformerFactory) {
        this.documentBuilderFactory = documentBuilderFactory;
        this.transformerFactory = transformerFactory;
    }

    public Set<Content> parse(InputStream is) throws IOException, ParserConfigurationException, SAXException {
        Set<Content> result = new HashSet<>();
        DocumentBuilder db = documentBuilderFactory.newDocumentBuilder();
        Document document = db.parse(is);
        Node uriAttribute = document.getAttributes().getNamedItem(ATTR_ID);
        if (uriAttribute != null) {
            result.add(toContent(document));
        } else {
            NodeList children = document.getChildNodes();
            range(0, children.getLength()).forEach(i -> result.add(toContent(children.item(i))));
        }
        return result;
    }

    private XmlContent toContent(Node node) {
        Node uriAttribute = node.getAttributes().getNamedItem(ATTR_ID);
        if (uriAttribute != null) {
            XmlContent single = new XmlContent(transformerFactory, node, collections(node));
            single.setUri(uriAttribute.getNodeValue());
            node.getAttributes().removeNamedItem(ATTR_ID);
            node.getAttributes().removeNamedItem(ATTR_COLLECTIONS);
            return single;
        }
        return null;
    }

    private Set<String> collections(Node node) {
        Node collectionAttribute = node.getAttributes().getNamedItem(ATTR_COLLECTIONS);
        if (collectionAttribute != null) {
            String[] collections = collectionAttribute.getNodeValue().split(",");
            return Arrays.asList(collections).stream().collect(Collectors.toSet());
        }
        return Collections.emptySet();
    }
}
