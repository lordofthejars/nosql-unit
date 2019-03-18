package com.lordofthejars.nosqlunit.marklogic.content;

import org.w3c.dom.Node;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public class XmlContent extends Content<Node> {

    public static final String ATTR_ID = "uri";

    public static final String ATTR_COLLECTIONS = "collections";

    protected Node data;

    private TransformerFactory transformerFactory;

    public XmlContent(Node data) {
        this.data = data;
    }

    XmlContent(TransformerFactory transformerFactory, Node data, Set<String> collections) {
        super(collections);
        this.transformerFactory = transformerFactory;
        this.data = data;
    }

    @Override
    public InputStream content() throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(data);
            StreamResult r = new StreamResult(out);
            transformer.transform(source, r);
            return new ByteArrayInputStream(out.toByteArray());
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public Node getData() {
        return data;
    }
}
