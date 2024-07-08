import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import simplejson as json
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy.exc import SQLAlchemyError
from kafka.errors import KafkaError

class RealTimeElectionDashboard:
    def __init__(self):
        self.topic_name = 'aggregated_votes_per_candidate'
        self.kafka_bootstrap_servers = 'localhost:9092'
        self.db_connection_string = "postgresql://postgres:postgres@localhost/voting"
        self.setup_sidebar()

    def create_kafka_consumer(self, topic_name):
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.kafka_bootstrap_servers,
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            return consumer
        except KafkaError as e:
            st.error(f"Error connecting to Kafka: {e}")
            return None

    @st.cache_data
    def fetch_voting_stats(_self):
        try:
            engine = create_engine(_self.db_connection_string)
            with engine.connect() as conn:
                voters_count = pd.read_sql("SELECT count(*) FROM voters", conn).iloc[0, 0]
                candidates_count = pd.read_sql("SELECT count(*) FROM candidates", conn).iloc[0, 0]
            return voters_count, candidates_count
        except SQLAlchemyError as e:
            st.error(f"Error fetching voting stats: {e}")
            return 0, 0

    def fetch_data_from_kafka(self, consumer):
        if not consumer:
            return []
        messages = consumer.poll(timeout_ms=1000)
        data = [sub_message.value for message in messages.values() for sub_message in message]
        return data

    def fetch_candidate_data(self):
        try:
            engine = create_engine(self.db_connection_string)
            with engine.connect() as conn:
                df = pd.read_sql("SELECT candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url FROM candidates", conn)
            return df
        except SQLAlchemyError as e:
            st.error(f"Error fetching candidate data: {e}")
            return pd.DataFrame()

    def plot_colored_bar_chart(self, results):
        data_type = results['candidate_name']
        colors = plt.cm.viridis(np.linspace(0, 1, len(data_type)))
        plt.bar(data_type, results['total_votes'], color=colors)
        plt.xlabel('Candidate')
        plt.ylabel('Total Votes')
        plt.title('Vote Counts per Candidate')
        plt.xticks(rotation=90)
        return plt

    def plot_donut_chart(self, data: pd.DataFrame, title='Donut Chart', type='candidate'):
        labels = list(data['candidate_name']) if type == 'candidate' else list(data['gender'])
        sizes = list(data['total_votes'])
        fig, ax = plt.subplots()
        ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
        ax.axis('equal')
        plt.title(title)
        return fig

    def plot_pie_chart(self, data, title='Gender Distribution of Voters', labels=None):
        sizes = list(data.values())
        labels = labels if labels else list(data.keys())
        fig, ax = plt.subplots()
        ax.pie(sizes, labels=labels, autopct='%1.1%%', startangle=140)
        ax.axis('equal')
        plt.title(title)
        return fig

    @st.cache_data(show_spinner=False)
    def split_frame(_self, input_df, rows):
        return [input_df.loc[i: i + rows - 1, :] for i in range(0, len(input_df), rows)]

    def paginate_table(self, table_data):
        top_menu = st.columns(3)
        with top_menu[0]:
            sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=1, index=1)
        if sort == "Yes":
            with top_menu[1]:
                sort_field = st.selectbox("Sort By", options=table_data.columns)
            with top_menu[2]:
                sort_direction = st.radio("Direction", options=["⬆️", "⬇️"], horizontal=True)
            table_data = table_data.sort_values(by=sort_field, ascending=sort_direction == "⬆️", ignore_index=True)

        pagination = st.container()
        bottom_menu = st.columns((4, 1, 1))
        with bottom_menu[2]:
            batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
        with bottom_menu[1]:
            total_pages = max(1, int(len(table_data) / batch_size))
            current_page = st.number_input("Page", min_value=1, max_value=total_pages, step=1)
        with bottom_menu[0]:
            st.markdown(f"Page **{current_page}** of **{total_pages}** ")

        pages = self.split_frame(table_data, batch_size)
        pagination.dataframe(data=pages[current_page - 1], use_container_width=True)

    def update_data(self):
        last_refresh = st.empty()
        last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

        voters_count, candidates_count = self.fetch_voting_stats()
        st.markdown("""---""")
        col1, col2 = st.columns(2)
        col1.metric("Total Voters", voters_count)
        col2.metric("Total Candidates", candidates_count)

        consumer = self.create_kafka_consumer("aggregated_votes_per_candidate")
        data = self.fetch_data_from_kafka(consumer)
        if not data:
            st.warning("No data available from Kafka.")
            return
        results = pd.DataFrame(data)

        candidate_data = self.fetch_candidate_data()
        if candidate_data.empty:
            st.warning("No candidate data available from the database.")
            return
        results = results.merge(candidate_data, on='candidate_id', how='left')

        results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
        leading_candidate = results.loc[results['total_votes'].idxmax()]

        st.markdown("""---""")
        st.header('Leading Candidate')
        col1, col2 = st.columns(2)
        
        with col1:
            st.image(leading_candidate['photo_url'], width=200)
        with col2:
            st.header(leading_candidate['candidate_name'])
            st.subheader(leading_candidate['party_affiliation'])
            st.subheader("Total Vote: {}".format(leading_candidate['total_votes']))

        st.markdown("""---""")
        st.header('Statistics')
        results = results[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes', 'photo_url']]
        results = results.reset_index(drop=True)
        col1, col2 = st.columns(2)
        with col1:
            bar_fig = self.plot_colored_bar_chart(results)
            st.pyplot(bar_fig)
        with col2:
            donut_fig = self.plot_donut_chart(results, title='Vote Distribution')
            st.pyplot(donut_fig)
        st.table(results)

        location_consumer = self.create_kafka_consumer("aggregated_turnout_by_location")
        location_data = self.fetch_data_from_kafka(location_consumer)
        if not location_data:
            st.warning("No location data available from Kafka.")
            return
        location_result = pd.DataFrame(location_data)

        if 'state' in location_result.columns:
            location_result = location_result.loc[location_result.groupby('state')['total_votes'].idxmax()]
            location_result = location_result.reset_index(drop=True)
            st.header("Location of Voters")
            self.paginate_table(location_result)
        else:
            st.error("The 'state' field is not found in the data from 'aggregated_turnout_by_location' topic.")

        st.session_state['last_update'] = time.time()

    def setup_sidebar(self):
        if st.session_state.get('last_update') is None:
            st.session_state['last_update'] = time.time()

        refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
        st_autorefresh(interval=refresh_interval * 1000, key="auto")

        if st.sidebar.button('Refresh Data'):
            self.update_data()

    def run(self):
        st.title('Real-time Election Dashboard')
        self.update_data()


if __name__ == '__main__':
    dashboard = RealTimeElectionDashboard()
    dashboard.run()
