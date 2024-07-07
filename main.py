from pg_utils import pgConfig,pgUtils
import random
from data_utils import DataGenerator
import simplejson as json
from utils import delivery_report
from kakfa_utils import kafkaUtils
from voting_processor import VotingProcessor

if __name__ == "__main__":
    BASE_URL = 'https://randomuser.me/api/?nat=gb'
    PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]

    random.seed(42)

    config = pgConfig(host='192.168.223.65', dbname='voting', username='postgres', password='postgres')
    pg_utils = pgUtils(config)
    pg_utils.execute_sql_file("create_table.sql")
    candidates = pg_utils.execute_query("select * from candidates")
    
    data_generator = DataGenerator(BASE_URL, PARTIES)
    voter_data = data_generator.generate_voter_data()

    #kafka-producer

    voters_topic = 'voters_topic'
    candidates_topic = 'candidates_topic'
    kafka_utils = kafkaUtils({'bootstrap.servers': 'localhost:9092', },"voting-group")
   
    if len(candidates) == 0:
        for i in range(3):
            candidate_data = data_generator.generate_candidate_data(i, 3)
            pg_utils.insert_record("candidates",candidate_data)

    if not kafka_utils.topic_is_empty(voters_topic):
        for i in range(100):
            voter_data = data_generator.generate_voter_data()

            pg_utils.insert_record("voters",voter_data)

            kafka_utils.produce_message(
                topic=voters_topic,
                key=voter_data["voter_id"],
                value=json.dumps(voter_data),
                on_delivery=delivery_report
            )
            print('Produced voter {}, data: {}'.format(i, voter_data))

    print("Produce jobs are done, start consuming voting data from kafka.")

    voting_processor = VotingProcessor(pg_utils, kafka_utils)
    voting_processor.process_votes()

    