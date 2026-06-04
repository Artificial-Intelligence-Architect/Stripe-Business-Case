import csv
import uuid
import random
from datetime import datetime, timedelta

merchants = [str(uuid.uuid4()) for _ in range(10)]
customers = [str(uuid.uuid4()) for _ in range(100)]
statuses = ['pending', 'success', 'failed', 'refunded', 'chargeback']
devices = ['mobile', 'desktop', 'tablet']
countries = ['US', 'FR', 'GB', 'DE', 'CA', 'JP', 'AU']

with open('transactions.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['transaction_id', 'merchant_id', 'customer_id', 'amount', 'currency', 
                     'amount_usd', 'payment_method', 'status', 'device_type', 'ip_country', 
                     'fraud_score', 'created_at'])
    for _ in range(500):
        txn_id = str(uuid.uuid4())
        merchant = random.choice(merchants)
        customer = random.choice(customers)
        amount = round(random.uniform(5, 5000), 2)
        currency = random.choice(['USD', 'EUR', 'GBP', 'JPY'])
        amount_usd = round(amount * (1.1 if currency == 'EUR' else 1.2), 2)
        payment = random.choice(['credit_card', 'paypal', 'apple_pay', 'google_pay'])
        status = random.choices(statuses, weights=[0.7, 0.25, 0.02, 0.02, 0.01])[0]
        device = random.choice(devices)
        country = random.choice(countries)
        fraud_score = round(random.uniform(0, 1), 4)
        created_at = datetime.now() - timedelta(days=random.randint(0, 60))
        writer.writerow([txn_id, merchant, customer, amount, currency, amount_usd, 
                         payment, status, device, country, fraud_score, created_at])