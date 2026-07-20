-- ============================================================
-- Données de démonstration minimales pour la validation runtime.
-- Juste assez pour prouver que : le schéma charge, les triggers
-- s'exécutent, le lignage refund fonctionne, et le RBAC mord.
-- ============================================================

-- Référentiel
INSERT INTO countries (code, name, region) VALUES
    ('US', 'United States', 'AMER'),
    ('FR', 'France', 'EMEA'),
    ('GB', 'United Kingdom', 'EMEA');

INSERT INTO currencies (code, name, usd_rate) VALUES
    ('USD', 'US Dollar', 1.000000),
    ('EUR', 'Euro', 1.080000),
    ('GBP', 'British Pound', 1.270000);

INSERT INTO product_categories (category_code, label, mcc) VALUES
    ('saas',    'Software / SaaS', '5734'),
    ('retail',  'Retail goods',    '5999');

-- Marchand + client (on fige les UUID pour pouvoir y référer ensuite)
INSERT INTO merchants (merchant_id, name, country_code, tier) VALUES
    ('11111111-1111-1111-1111-111111111111', 'Acme SaaS', 'US', 'growth');

INSERT INTO customers (merchant_id, customer_id, email, email_hash, country_code) VALUES
    ('11111111-1111-1111-1111-111111111111',
     '22222222-2222-2222-2222-222222222222',
     'alice@example.com',
     encode(digest('alice@example.com','sha256'),'hex'),
     'US');

INSERT INTO products (merchant_id, product_id, name, category_code, unit_amount, currency, is_recurring) VALUES
    ('11111111-1111-1111-1111-111111111111',
     '33333333-3333-3333-3333-333333333333',
     'Pro Plan', 'saas', 49.00, 'USD', true);

-- Un abonnement + son événement de création
INSERT INTO subscriptions (merchant_id, subscription_id, customer_id, product_id,
    status, billing_interval, unit_amount, currency,
    current_period_start, current_period_end) VALUES
    ('11111111-1111-1111-1111-111111111111',
     '44444444-4444-4444-4444-444444444444',
     '22222222-2222-2222-2222-222222222222',
     '33333333-3333-3333-3333-333333333333',
     'active', 'month', 49.00, 'USD',
     now(), now() + INTERVAL '1 month');

INSERT INTO subscription_events (merchant_id, subscription_id, event_type,
    from_status, to_status, mrr_delta_usd) VALUES
    ('11111111-1111-1111-1111-111111111111',
     '44444444-4444-4444-4444-444444444444',
     'created', NULL, 'active', 49.00);

-- Un PAIEMENT. Le trigger FX doit remplir amount_usd, le trigger d'audit
-- doit écrire une ligne dans audit_log. C'est LE test du bug corrigé :
-- avant, cet INSERT plantait.
INSERT INTO transactions (merchant_id, transaction_id, created_at, customer_id,
    kind, subscription_id, product_id, amount, currency, payment_method, status) VALUES
    ('11111111-1111-1111-1111-111111111111',
     '55555555-5555-5555-5555-555555555555',
     now(),
     '22222222-2222-2222-2222-222222222222',
     'payment',
     '44444444-4444-4444-4444-444444444444',
     '33333333-3333-3333-3333-333333333333',
     49.00, 'USD', 'card', 'success');

-- Un REMBOURSEMENT partiel qui référence le paiement ci-dessus.
-- fn_validate_parent_txn() doit accepter (20 <= 49) puis le trigger d'audit
-- doit logger. Prouve le lignage refund → parent.
INSERT INTO transactions (merchant_id, transaction_id, created_at, customer_id,
    kind, parent_transaction_id, parent_created_at,
    amount, currency, payment_method, status) VALUES
    ('11111111-1111-1111-1111-111111111111',
     '66666666-6666-6666-6666-666666666666',
     now(),
     '22222222-2222-2222-2222-222222222222',
     'refund',
     '55555555-5555-5555-5555-555555555555',
     (SELECT created_at FROM transactions
      WHERE transaction_id = '55555555-5555-5555-5555-555555555555'),
     20.00, 'USD', 'card', 'success');
