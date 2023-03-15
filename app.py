from flask import Flask, render_template, request, redirect
from flask_sqlalchemy import SQLAlchemy
from kafka import KafkaProducer
import json

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///transactions.db'
db = SQLAlchemy(app)

class Transaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sender = db.Column(db.String(100), nullable=False)
    recipient = db.Column(db.String(100), nullable=False)
    amount = db.Column(db.Float, nullable=False)

with app.app_context():
    db.create_all()

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        sender = request.form['sender']
        recipient = request.form['recipient']
        amount = float(request.form['amount'])

        new_transaction = Transaction(sender=sender, recipient=recipient, amount=amount)
        db.session.add(new_transaction)
        db.session.commit()

        producer.send('transactions', {'sender': sender, 'recipient': recipient, 'amount': amount})
        producer.flush()

        return redirect('/')
    else:
        return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
