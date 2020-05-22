package com.andrew.sparkwork.utils;

import com.andrew.sparkwork.utils.PropertyUtils;
import org.junit.Test;
import org.junit.Assert;

import java.util.Properties;


public class PropertyUtilsTest {


    @Test
    public void loadPropertyTest() throws Exception
    {
        PropertyUtilsTest pt = new PropertyUtilsTest();


        Properties props = PropertyUtils.loadPropertiesFromArgs( new String[]{"src/test/resources/test.properties"});

        Assert.assertEquals("a,b", props.getProperty("steps"));

    }
}
