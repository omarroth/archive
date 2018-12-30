CREATE TABLE public.workers (
    id uuid NOT NULL,
    ip text,
    reputation integer,
    disabled boolean,
    current_batch uuid,
    last_committed timestamp with time zone
);

ALTER TABLE ONLY public.workers
    ADD CONSTRAINT workers_pkey PRIMARY KEY (id);

GRANT ALL ON TABLE public.workers TO kemal;
