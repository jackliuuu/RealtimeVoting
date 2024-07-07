import random
import time
from datetime import datetime
from pg_utils import pgUtils
from kakfa_utils import kafkaUtils
from confluent_kafka import KafkaException
from utils import delivery_report
import simplejson as json

class VotingProcessor:
    def __init__(self, db_manager: pgUtils, kafka_manager: kafkaUtils):
        self.db_manager = db_manager
        self.kafka_manager = kafka_manager

    def process_votes(self):
        candidates = self.db_manager.execute_query('''
            SELECT row_to_json(t)
                    FROM (
                        SELECT * FROM candidates
                    ) t;
            ''')
        candidates = [candidate[0] for candidate in candidates]
        if not candidates:
            raise Exception("No candidates found in database")
   

        try:
            for voter in self.kafka_manager.consume_messages('voters_topic'):
                chosen_candidate = random.choice(candidates)
                voter = json.loads(voter)
                vote = {
                    "voter_id" : voter['voter_id'],
                    "candidate_id" : chosen_candidate["candidate_id"],
                    "voting_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "vote": 1
                }
                try:
                    print("User {} is voting for candidate: {}".format(vote['voter_id'], vote['candidate_id']))
                    self.db_manager.execute_insert(f"""
                            INSERT INTO votes (voter_id, candidate_id, voting_time)
                            VALUES ('{vote['voter_id']}','{vote['candidate_id']}', '{vote['voting_time']}')
                        """)
                    self.kafka_manager.produce_message(
                        'votes_topic',
                        key=vote["voter_id"],
                        value=vote,
                        on_delivery=delivery_report
                    )
                except Exception as e:
                    print("Error: {}".format(e))
                    continue
                time.sleep(0.2)
        except KafkaException as e:
            print(e)
