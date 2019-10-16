# ./runDataFlow.sh $PROJECT_ID $BUCKET $DATASET [Beam File Absolute Path]

# Required options for pipeline_args
# job_name - The name of the Cloud Dataflow job being executed.
# project - The ID of your GCP project.
# runner - The pipeline runner that will parse your program and construct your pipeline. 
#          For cloud execution, this must be DataflowRunner.
# staging_location - A Cloud Storage path for Cloud Dataflow to stage code packages 
#                    needed by workers executing the job. If not specified, use temp_loc
# temp_location - A Cloud Storage path for Cloud Dataflow to stage temporary job files 
#                 created during the execution of the pipeline.


DATASET=buildings
LOAD_SUFFIX=energy_history
TOPIC_IN=energy
TOPIC_OUT=energy_avgs

echo "Launching mainPipeline.py project=$PROJECT_ID bucket=$BUCKET for BQ dataset=buildings and PubSub topic=energy"

python ./dataflow/mainPipeline.py \
    --runner DataflowRunner \
    --project $PROJECT_ID \
    --temp_location gs://$BUCKET/tmp/ \
    --streaming \
    --input_topic projects/$PROJECT_ID/topics/$TOPIC_IN \
    --output_load_table_suffix $DATASET.$LOAD_SUFFIX \
    --output_stream_table $DATASET.$TOPIC_OUT \
    --output_topic $TOPIC_OUT \
    --speedFactor 300
