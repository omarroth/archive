CREATE TABLE public.batches (
    id uuid NOT NULL,
    start_ctid text,
    end_ctid text,
    finished boolean
);

ALTER TABLE ONLY public.batches
    ADD CONSTRAINT batches_pkey PRIMARY KEY (id);

GRANT ALL ON TABLE public.batches TO kemal;
