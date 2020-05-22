package com.andrew.sparkwork.trans;

import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.feature.*;

public class MLLibTransformer  extends AbstractTransformer {

    private static final Logger log = LoggerFactory.getLogger(MLLibTransformer.class);

    public MLLibTransformer(Properties p) {
        super(p);
    }

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> inputs, SparkSession session, String stepName) throws Exception {
        if (inputs != null) {
            log.info("Total inputs: {}", inputs.size());
        }

        if( !props.containsKey("mllib.pipeline") ) {
            throw new RuntimeException("We need pipeline defined for MLLIB algorithm");
        }

        String[] pipelineNames = props.getProperty("mllib.pipeline").split(",");
        PipelineStage[] pipelineStages = new PipelineStage[pipelineNames.length];

        for( int i=0; i<pipelineNames.length; i++) {
            String[] pipelineName = pipelineNames[i].split(":");
            if(pipelineName.length !=2 ) {
                throw new RuntimeException("Pipeline Definition Error: <pipelineObject>:<PipelineClassName");
            }

            Class<?> clazz;

            try {
                clazz = Class.forName(pipelineName[1]);
            } catch( ClassNotFoundException e ) {
                clazz = Class.forName( "org.apache.spark.ml.feature." + pipelineName[1]);
            }
            Constructor<?> constructor = clazz.getConstructor();

            pipelineStages[i] = (PipelineStage) constructor.newInstance();

            String pipelineProps = "mllib.pipeline."+pipelineName[0];

            String[] methods = props.getProperty(pipelineProps).split(",");

            for (String method:
                 methods) {
                log.info("What is this???????????????????????????????? {}", method);
            }
        }

        Pipeline pipeline = new Pipeline();
        pipeline.setStages(pipelineStages);

        return null;
    }
}