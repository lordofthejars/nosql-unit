package com.lordofthejars.nosqlunit.marklogic.content;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
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
import static org.w3c.dom.Node.ELEMENT_NODE;

public class XmlParser {

    private static final String COLLECTIONS_SEPARATOR = ",";

    private DocumentBuilderFactory documentBuilderFactory;

    private TransformerFactory transformerFactory;

    public XmlParser(DocumentBuilderFactory documentBuilderFactory, TransformerFactory transformerFactory) {
        this.documentBuilderFactory = documentBuilderFactory;
        this.transformerFactory = transformerFactory;
    }

    public Set<Content> parse(InputStream is) throws IOException, ParserConfigurationException, SAXException {
        Set<Content> result = new HashSet<>();
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        Document document = documentBuilder.parse(is);
        document.normalizeDocument();
        Element root = document.getDocumentElement();
        if (root.hasAttribute(ATTR_ID)) {
            result.add(toContent(root));
        } else { //skip the root element and iterate over children
            for (Node child = root.getFirstChild(); child != null; child = child.getNextSibling()) {
                if (ELEMENT_NODE == child.getNodeType()) {
                    result.add(toContent(child));
                }
            }
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
            String[] collections = collectionAttribute.getNodeValue().split(COLLECTIONS_SEPARATOR);
            return Arrays.asList(collections).stream().collect(Collectors.toSet());
        }
        return Collections.emptySet();
    }
}
