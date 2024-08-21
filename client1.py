import requests

def get_sentiment(text):
    url = 'http://127.0.0.1:5000/analyze'
    payload = {'text': text}
    response = requests.post(url, data=payload)
    
    if response.status_code == 200:
        return response.json()
    else:
        print("Error:", response.status_code)
        print(response.text)  # Print response text for debugging
        return None

if __name__ == '__main__':
    text = input("Enter text to analyze: ")
    sentiment = get_sentiment(text)
    if sentiment:
        print(sentiment)