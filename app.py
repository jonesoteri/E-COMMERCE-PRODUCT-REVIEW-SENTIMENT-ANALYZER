from flask import Flask, render_template, request, jsonify
import pickle
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Load the VADER sentiment analyzer
with open("model/vader_analyzer.pkl", 'rb') as file:
    analyzer = pickle.load(file)

app = Flask(__name__)

def analyze_sentiment(text):
    if not text:
        return "", ""
    sentiment = analyzer.polarity_scores(text)
    score = sentiment['compound']
    
    if score >= 0.05:
        return "Positive", "😊"
    elif score <= -0.05:
        return "Negative", "😢"
    else:
        return "Neutral", "😐"
    
@app.route("/")
def home():
    return render_template("index.html")
    
@app.route('/predict', methods=['POST'])
def predict():
    email = request.form.get('content')
    prediction, emoji = analyze_sentiment(email)
    return render_template("index.html", prediction=prediction, emoji=emoji, email=email)

@app.route('/api/predict', methods=['POST'])
def predict_api():
    data = request.get_json(force=True)
    email = data['content']
    prediction, emoji = analyze_sentiment(email)
    return jsonify({'prediction': prediction, 'emoji': emoji, 'email': email})

if __name__=="__main__":
    app.run(debug=True)
