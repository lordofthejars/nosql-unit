
package com.lordofthejars.nosqlunit.influxdb;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.lordofthejars.nosqlunit.influxdb.matchers.ReflectiveFieldMatcher;

public class WhenInfluxDbOperationsAreRequired {

    private static final String DATA = "{\"measurement0\": [" +
            "        {" +
            "            \"tags\": {" +
            "                \"tag1\": \"value0\"" +
            "            }," +
            "            \"time\": 1514764800000000000," +
            "            \"precision\": \"NANOSECONDS\"," +
            "            \"fields\": {" +
            "                \"field1\": \"field value10\"," +
            "                \"field2\": \"field value20\"" +
            "            }" +
            "        }" +
            "    ]," +
            "    \"measurement1\": [" +
            "        {" +
            "            \"tags\": {" +
            "                \"tag1\": \"value1\"" +
            "            }," +
            "            \"time\": 1514764800000000001," +
            "            \"precision\": \"NANOSECONDS\"," +
            "            \"fields\": {" +
            "                \"field1\": \"field value11\"," +
            "                \"field2\": \"field value21\"" +
            "            }" +
            "        }" +
            "    ]}";

    @Mock
    private InfluxDB influx;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        doNothing().when(influx).write(any(Point.class));
    }

    @Test
    public void insert_opertation_should_add_data_into_tables() throws UnsupportedEncodingException {

        final InfluxOperation influxOperation = new InfluxOperation(influx);
        influxOperation.insert(new ByteArrayInputStream(DATA.getBytes("UTF-8")));

        final ArgumentCaptor<Point> insertCaptor = ArgumentCaptor.forClass(Point.class);
        verify(influx, times(2)).write(insertCaptor.capture());

        final List<Point> allValues = insertCaptor.getAllValues();
        assertThat(allValues, hasSize(2));

        for (int i = 0; i < allValues.size(); i++) {
            final Point point = allValues.get(i);
            assertThat(point, is(notNullValue()));
            assertThat(point, new ReflectiveFieldMatcher<>("measurement", is("measurement" + i)));
            assertThat(point, new ReflectiveFieldMatcher<>("time", is(1514764800000000000L + i)));
            assertThat(point, new ReflectiveFieldMatcher<>("precision", is(TimeUnit.NANOSECONDS)));
            assertThat(point, new ReflectiveFieldMatcher<>("tags", hasEntry("tag1", "value" + i)));
            assertThat(point, new ReflectiveFieldMatcher<>("fields", hasEntry("field1", "field value1" + i)));
            assertThat(point, new ReflectiveFieldMatcher<>("fields", hasEntry("field2", "field value2" + i)));
        }
    }

}
