import requests
import time

# Configurations
AUTH_URL = "https://dev-api.talkingtotals.ai/api/v1/users/authenticate"  # Endpoint to authenticate and get JWT
POST_URL = "https://dev-api.talkingtotals.ai/api/v2/chats"     # Endpoint to post each line
FILE_PATH = "test_case_payable_rec.txt"

MOBILE = "9999999999"
PASSWORD = "1234"
#Fri, Jul 11, 2025 03:34:000045 -> {message: what is, return_as_file: 1, row_threshold: 25, file_type: pdf}

sessionId = None 

def get_jwt_token(mobile, password):
    payload = {
		"dial_code" : 91,
        "mobile_number": mobile,
        "password": password
    }
    try:
        response = requests.post(AUTH_URL, json=payload)
        response.raise_for_status()
        token = response.json().get("token")
        return token
    except requests.exceptions.RequestException as e:
        print("Authentication failed:", e)
        return None

def post_line(line, token):
    global sessionId
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-Session-ID" : sessionId
    }
    payload = {
        "message" : line.strip(), 
        "return_as_file" : 1, 
        "row_threshold" : 5, 
        "file_type": "pdf"
    }
    payload = { "message": line.strip(),
        "return_as_file": False,
        "row_threshold": 20,
        "use_context": 0,
        "use_ner": 1,
        "use_vector_search": 0,
    }

    try:
        response = requests.post(POST_URL, json=payload, headers=headers)
        print(f"Posted: {line.strip()} | Status: {response.status_code} | response: {response.content}")
        sessionId = response.json().get('session_id')
    except requests.exceptions.RequestException as e:
        print(f"Failed to post line: {line.strip()} | Error: {e}")

def main():
    token = get_jwt_token(MOBILE, PASSWORD)
    if not token:
        print("Exiting: No token received.")
        return
    print(token)
    post_line("What is the balance of Aereo Mfg?", token)
    # with open(FILE_PATH, 'r') as file:
    #     for line in file:
    #         if line.strip() and not line.startswith('#'):  # Skip empty lines
    #             time.sleep(5)
    #             post_line(line, token)

if __name__ == "__main__":
    main()