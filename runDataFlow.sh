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



TOPIC_IN=energy
TOPIC_OUT=energy_avgs

echo "$BUCKET"
echo "Launching currentConsumption.py project=$PROJECT_ID bucket=$BUCKET for BQ dataset=$DATASET and PubSub topic=$TOPIC"

# python $MAIN \
#     --runner Dataflow Runner \
#     --project $PROJECT \
#     --temp_location gs://$BUCKET/tmp/ \
#     --streaming True \
#     --input_topic projects/$PROJECT/topics/$TOPIC_IN\
#     --output_table_load $DATASET.energy_history\
#     --output_table_stream $DATASET.energy_avgs\ 
#     --output_topic projects/$PROJECT/topics/$TOPIC_OUT\

python ./dataflow/currentConsumption.py \
    --runner DataflowRunner \
    --project $PROJECT_ID \
    --temp_location gs://$BUCKET/tmp/ \
    --streaming true \
    --input_topic projects/$PROJECT_ID/topics/energy \
    --output_load_table_suffix buildings.energy_history \
    --output_stream_table buildings.energy_avgs \
    --output_topic energy_avgs
