#    Copyright 2019 Julie Jung <moonyouj889@gmail.com>

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.


from google.cloud import pubsub_v1
import argparse


# TODO project_id = "Your Google Cloud Project ID"
# TODO topic_name = "Your Pub/Sub topic name"
# TODO subscription_name = "Your Pub/Sub subscription name"

# project_id = 'qwiklabs-gcp-9a7649e156f8f9f5'
# topic_name = 'energy'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Create simple subscription to matching project ID and topic')
    parser.add_argument('--project_id', required=True, type=str)
    parser.add_argument('--topic', required=True, type=str)
    parser.add_argument('--sub_name', required=True, type=str)
    args = parser.parse_args()
    project_id = args.project_id
    topic_name = args.topic
    subscription_name = args.sub_name

    subscriber = pubsub_v1.SubscriberClient()
    topic_path = subscriber.topic_path(project_id, topic_name)
    subscription_path = subscriber.subscription_path(
        project_id, subscription_name)

    subscription = subscriber.create_subscription(
        subscription_path, topic_path)

    print('Subscription created: {}'.format(subscription))
