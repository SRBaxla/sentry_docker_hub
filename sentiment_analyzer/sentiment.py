from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
from fastapi import FastAPI

app=FastAPI()
# Load FinBERT once
tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
model.eval()

@app.get('/')
def home():
    return{"status":"ok","msg":"Sentiment analyzer"}

@app.get('/analyze')
def analyze_sentiment(text: str) -> dict:
    inputs = tokenizer(text, return_tensors="pt", truncation=True)
    with torch.no_grad():
        outputs = model(**inputs)
        scores = torch.nn.functional.softmax(outputs.logits, dim=1)[0]
    return {
        "negative": float(scores[0]),
        "neutral":  float(scores[1]),
        "positive": float(scores[2]),
    }
