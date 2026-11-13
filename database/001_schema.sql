-- Execute somente em um banco reservado para este desafio.
CREATE TABLE IF NOT EXISTS public.customers (
    id BIGINT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, document TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS public.accounts (
    id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL REFERENCES public.customers(id),
    status TEXT NOT NULL, balance NUMERIC(15,2) NOT NULL,
    updated_at TIMESTAMP(3) NOT NULL
);
CREATE TABLE IF NOT EXISTS public.cards (
    id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL REFERENCES public.accounts(id),
    token TEXT NOT NULL, last4 CHAR(4) NOT NULL, status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS public.merchants (
    id BIGINT PRIMARY KEY, name TEXT NOT NULL, category TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS public.transactions (
    id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL REFERENCES public.accounts(id),
    card_id BIGINT NOT NULL REFERENCES public.cards(id),
    merchant_id BIGINT NOT NULL REFERENCES public.merchants(id),
    amount NUMERIC(15,2) NOT NULL CHECK (amount > 0),
    currency CHAR(3) NOT NULL, status TEXT NOT NULL, occurred_at TIMESTAMP(3) NOT NULL
);
CREATE INDEX IF NOT EXISTS transactions_card_time ON public.transactions(card_id, occurred_at);
ALTER TABLE public.customers REPLICA IDENTITY FULL;
ALTER TABLE public.accounts REPLICA IDENTITY FULL;
ALTER TABLE public.cards REPLICA IDENTITY FULL;
ALTER TABLE public.merchants REPLICA IDENTITY FULL;
ALTER TABLE public.transactions REPLICA IDENTITY FULL;
