package com.andrew.sparkwork.trans;

import com.andrew.sparkwork.utils.HDFSUtils;
import com.andrew.sparkwork.utils.PropertyUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RandomSplitDataframeTransformer extends AbstractTransformer
{

    public RandomSplitDataframeTransformer(Properties p)
    {
        super(p);
    }

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> map, SparkSession session, String stepName) throws Exception {

        Map<String, Dataset<Row>> datasetsMap = new HashMap<>();
        if( map.size() == 1 ) {
            Dataset<Row> dataset = map.values().iterator().next();

            int randomSeed = Integer.valueOf(props.getProperty("randomSeed", "123456") );
            String[] ratioString = props.getProperty("dataframe.ratio", "0.8,0.2").split(",");

            double[] ratioArray = new double[ratioString.length];
            for( int i=0; i<ratioString.length; i++) {
                ratioArray[i] = Double.valueOf(ratioString[i]);
            }

            if( props.containsKey("dataframe.name") ) {
              String[] dataframeString = props.getProperty("dataframe.name").split(",");
              if( dataframeString.length != ratioArray.length ) {
                  throw new RuntimeException("SplitTransformer: splitted frames should match the size of names.");
              }

              Dataset<Row>[] datasets = dataset.randomSplit(ratioArray, randomSeed);
              for( int ii=0; ii<ratioArray.length; ii++) {
                  datasetsMap.put( stepName + "_" + dataframeString[ii], datasets[ii]);
              }
            } else {
                Dataset<Row>[] datasets = dataset.randomSplit(ratioArray, randomSeed);
                for( int ii=0; ii<ratioArray.length; ii++) {
                  datasetsMap.put( stepName + "_" + ii, datasets[ii]);
                }
            }

            return datasetsMap;
        } else {
            throw new RuntimeException("SplitTransformer should only have one input dataframe.");
        }
    }

}

