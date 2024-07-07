import requests
import random

class DataGenerator:
    def __init__(self, base_url, parties):
        self.base_url = base_url
        self.parties = parties
        random.seed(42)

    def fetch_user_data(self, gender=None):
        url = self.base_url
        if gender:
            url += '&gender=' + gender
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['results'][0]
        else:
            raise Exception("Error fetching data")

    def generate_voter_data(self):
        user_data = self.fetch_user_data()
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }

    def generate_candidate_data(self, candidate_number, total_parties):
        gender = 'female' if candidate_number % 2 == 1 else 'male'
        user_data = self.fetch_user_data(gender)
        return {
            "candidate_id": user_data['login']['uuid'],
            "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "party_affiliation": self.parties[candidate_number % total_parties],
            "biography": "A brief bio of the candidate.",
            "campaign_platform": "Key campaign promises or platform.",
            "photo_url": user_data['picture']['large']
        }
